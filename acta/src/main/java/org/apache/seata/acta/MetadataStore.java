/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.acta;

import org.apache.seata.acta.Metadata.Decision;
import org.apache.seata.acta.Metadata.Delivery;
import org.apache.seata.acta.Metadata.InboxEntry;
import org.apache.seata.acta.Metadata.Message;
import org.apache.seata.acta.Metadata.NoVote;
import org.apache.seata.acta.Metadata.OutboxEntry;
import org.apache.seata.acta.Metadata.Phase2;
import org.apache.seata.acta.Metadata.Position;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

// Durable Inbox, Outbox, Epoch, and RetireWatermark collections of one service, stored in its local database
// (Section 3.2). Every method is one individual metadata operation or one atomic block, and
// each atomic block runs as one short local transaction that commits independently of application transactions,
// on plain connections of the service's ordinary data source, never on the Seata XA proxy. The tid columns hold the
// text form of the attempt's Seata XA branch identifier (XaBranches). Table names are the configured prefix
// (seata.acta.table-prefix, default acta_) plus inbox, outbox, retire_watermark, service, and peer.
public class MetadataStore {

    public enum Dialect {MYSQL, POSTGRESQL}

    private static final String DEFAULT_PREFIX = "acta_";

    // Create table language: 4 metadata tables (written with the default prefix, rewritten to the configured one)
    private static final String[] MYSQL_DDL = {
            "create table if not exists acta_service (singleton tinyint not null primary key, id char(36) not null,"
                    + " epoch bigint not null)",
            "create table if not exists acta_inbox (id bigint not null primary key, gtid varchar(128) not null,"
                    + " source char(36) not null, target char(36) not null, epoch bigint not null, seq bigint not null,"
                    + " body blob not null, tid varchar(192) null, no_vote varchar(8) null, phase2 varchar(16) null)",
            "create table if not exists acta_outbox (id bigint not null primary key, gtid varchar(128) not null,"
                    + " source char(36) not null, target char(36) not null, epoch bigint not null, seq bigint not null,"
                    + " body blob not null, in_id bigint not null, tid varchar(192) not null, delivery varchar(8) not null,"
                    + " index acta_outbox_target_pos (target, epoch, seq), index acta_outbox_in (in_id),"
                    // listNewOutbox: without it every poll round scans the whole table, mostly deleted rows, which
                    // delays commit requests and so lengthens the prepared window and its row-lock waits.
                    + " index acta_outbox_delivery_pos (delivery, epoch, seq))",
            "create table if not exists acta_retire_watermark (source char(36) not null primary key,"
                    + " epoch bigint not null, seq bigint not null)",
            // Persistent UUIDs of the other services, so that a restarted service needs no peer to be online.
            "create table if not exists acta_peer (db varchar(16) not null primary key, id char(36) not null)"};

    private static final String[] POSTGRESQL_DDL = {
            "create table if not exists acta_service (singleton smallint not null primary key, id char(36) not null,"
                    + " epoch bigint not null)",
            "create table if not exists acta_inbox (id bigint not null primary key, gtid varchar(128) not null,"
                    + " source char(36) not null, target char(36) not null, epoch bigint not null, seq bigint not null,"
                    + " body bytea not null, tid varchar(192) null, no_vote varchar(8) null, phase2 varchar(16) null)",
            "create table if not exists acta_outbox (id bigint not null primary key, gtid varchar(128) not null,"
                    + " source char(36) not null, target char(36) not null, epoch bigint not null, seq bigint not null,"
                    + " body bytea not null, in_id bigint not null, tid varchar(192) not null, delivery varchar(8) not null)",
            "create index if not exists acta_outbox_target_pos on acta_outbox (target, epoch, seq)",
            "create index if not exists acta_outbox_in on acta_outbox (in_id)",
            "create index if not exists acta_outbox_delivery_pos on acta_outbox (delivery, epoch, seq)", // listNewOutbox
            "create table if not exists acta_retire_watermark (source char(36) not null primary key,"
                    + " epoch bigint not null, seq bigint not null)",
            "create table if not exists acta_peer (db varchar(16) not null primary key, id char(36) not null)"};

    private static final String INBOX_COLUMNS = "id, gtid, source, target, epoch, seq, body, tid, no_vote, phase2";
    private static final String OUTBOX_COLUMNS = "id, gtid, source, target, epoch, seq, body, in_id, tid, delivery";

    // A source's RetireWatermark row is the serialization point of receive and advanceWatermark: receive holds a
    // shared lock on it from its watermark check until its inbox insert commits, and advanceWatermark's update
    // needs the exclusive lock, so it waits for those receives, and every receive that starts afterwards reads the
    // advanced watermark. Hence a message retired by a committed watermark can never be inserted later, which is
    // what lets tryGcInbox delete it (Figure 5 lines 101-106, Figure 6 lines 135-138). The row is created with
    // this sentinel, below every real position, before the first message from a source is received or retired,
    // so the first receptions and the first advance lock the same row too; readers report it as "nothing retired".
    private static final Position NONE_RETIRED = new Position(-1, -1);

    private final DataSource dataSource;
    private final Dialect dialect;
    private final String prefix;
    private final String inbox;
    private final String outbox;
    private final String watermark;
    private final String service;
    private final String peer;
    private volatile UUID serviceId;

    public MetadataStore(DataSource dataSource, Dialect dialect, String tablePrefix) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.prefix = tablePrefix;
        this.inbox = tablePrefix + "inbox";
        this.outbox = tablePrefix + "outbox";
        this.watermark = tablePrefix + "retire_watermark";
        this.service = tablePrefix + "service";
        this.peer = tablePrefix + "peer";
    }

    /*
    Service identity and Epoch (Section 3.2, Figure 7 lines 140-142)
     */

    // Creates the metadata tables and the persistent service UUID with Epoch 0 if they do not exist yet.
    public void initialize() {
        String[] ddl = dialect == Dialect.MYSQL ? MYSQL_DDL : POSTGRESQL_DDL;
        withConnection(connection -> {
            for (String statement : ddl) {
                try (var stmt = connection.createStatement()) {
                    stmt.execute(statement.replace(DEFAULT_PREFIX, prefix));
                }
            }
            return null;
        });
        serviceId = inTransaction(connection -> {
            try (var select = connection.prepareStatement("select id from " + service + " where singleton = 1")) {
                try (ResultSet rs = select.executeQuery()) {
                    if (rs.next()) {
                        return UUID.fromString(rs.getString(1));
                    }
                }
            }
            UUID fresh = UUID.randomUUID();
            try (var insert = connection.prepareStatement(
                    "insert into " + service + " (singleton, id, epoch) values (1, ?, 0)")) {
                insert.setString(1, fresh.toString());
                insert.executeUpdate();
            }
            return fresh;
        });
    }

    // Returns the persistent UUID that identifies this service across restarts.
    public UUID serviceId() {
        if (serviceId == null) {
            throw new IllegalStateException("MetadataStore is not initialized");
        }
        return serviceId;
    }

    // Atomically increments the durable Epoch and returns the new value (Figure 7 lines 140-142).
    public long incrementEpoch() {
        return inTransaction(connection -> {
            try (var update = connection.prepareStatement(
                    "update " + service + " set epoch = epoch + 1 where singleton = 1")) {
                update.executeUpdate();
            }
            try (var select = connection.prepareStatement("select epoch from " + service + " where singleton = 1");
                    ResultSet rs = select.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException(service + " row is missing");
                }
                return rs.getLong(1);
            }
        });
    }

    /*
    Inbox
     */

    // Reads Inbox[id]; empty when the entry is absent (e.g., Figure 5 line 91, Figure 7 line 147).
    public Optional<InboxEntry> readInbox(long id) {
        return withConnection(connection -> {
            try (var select = connection.prepareStatement(
                    "select " + INBOX_COLUMNS + " from " + inbox + " where id = ?")) { // search according to the id
                select.setLong(1, id);
                try (ResultSet rs = select.executeQuery()) {
                    return rs.next() ? Optional.of(readInboxEntry(rs)) : Optional.empty(); // Read and assemble into InboxEntry
                }
            }
        });
    }

    // Lists every durable inbox entry, for input recovery and inbox garbage collection (Sections 3.4 and 3.5).
    public List<InboxEntry> listInbox() {
        return withConnection(connection -> {
            try (var select = connection.prepareStatement("select " + INBOX_COLUMNS + " from " + inbox);
                    ResultSet rs = select.executeQuery()) {
                List<InboxEntry> entries = new ArrayList<>();
                while (rs.next()) {
                    entries.add(readInboxEntry(rs));
                }
                return entries;
            }
        });
    }

    // Lists inputs no attempt of which has been published, voted no, or entered phase 2, for the inbox dispatcher.
    public List<InboxEntry> listPendingInbox() {
        return withConnection(connection -> {
            try (var select = connection.prepareStatement("select " + INBOX_COLUMNS
                    + " from " + inbox + " where tid is null and no_vote is null and phase2 is null");
                    ResultSet rs = select.executeQuery()) {
                List<InboxEntry> entries = new ArrayList<>();
                while (rs.next()) {
                    entries.add(readInboxEntry(rs));
                }
                return entries;
            }
        });
    }

    // Lists root inputs (source CLIENT) whose global decision has been applied, for client outcome delivery (A8).
    public List<InboxEntry> listDecidedRoots() {
        return withConnection(connection -> {
            try (var select = connection.prepareStatement("select " + INBOX_COLUMNS
                    + " from " + inbox + " where source = ? and phase2 in (?, ?)")) {
                select.setString(1, Metadata.CLIENT.toString());
                select.setString(2, Phase2.COMMITTED.name());
                select.setString(3, Phase2.ABORTED.name());
                try (ResultSet rs = select.executeQuery()) {
                    List<InboxEntry> entries = new ArrayList<>();
                    while (rs.next()) {
                        entries.add(readInboxEntry(rs));
                    }
                    return entries;
                }
            }
        });
    }

    // Atomically inserts a received message with NULL tid, no-vote, and phase-2 states unless its position is
    // at or below the source's retire watermark or the entry already exists (Figure 5 lines 101-106). The watermark
    // is read under a shared row lock held until commit, so no advanceWatermark can commit in between (see
    // NONE_RETIRED).
    public void receive(Message msg) {
        inTransaction(connection -> { // atomic
            ensureWatermarkRow(connection, msg.source(), false);
            Optional<Position> watermark = lockWatermarkShared(connection, msg.source());
            if (watermark.isPresent() && msg.pos().compareTo(watermark.get()) <= 0) {
                return null;
            }
            // Remove duplicates during insertion
            String insert = dialect == Dialect.MYSQL //tid, no_vote, phase2 = null
                    ? "insert into " + inbox + " (" + INBOX_COLUMNS + ") values (?, ?, ?, ?, ?, ?, ?, null, null, null)"
                            + " on duplicate key update id = id" // MySQL
                    : "insert into " + inbox + " (" + INBOX_COLUMNS + ") values (?, ?, ?, ?, ?, ?, ?, null, null, null)"
                            + " on conflict (id) do nothing"; //PG
            try (var stmt = connection.prepareStatement(insert)) {
                bindMessage(stmt, msg); // Bind: id, gtid, source, target, epoch, seq, body
                stmt.executeUpdate();
            }
            return null;
        });
    }

    // Atomically records ABORTED for an input whose registration learned an abort, if present (Figure 3 lines 19-22).
    public void markAbortedIfPresent(long inId) {
        updateInbox("update " + inbox + " set phase2 = ? where id = ?", Phase2.ABORTED.name(), inId);
    }

    // Atomically records the attempt's tid and its complete output set; returns false without writing when the
    // input is absent or already ABORTING or ABORTED (Figure 3 lines 30-38).
    public boolean publishAttempt(long inId, String tid, List<Message> outputs) {
        return inTransaction(connection -> {
            try (var select = connection.prepareStatement(
                    "select phase2 from " + inbox + " where id = ? for update")) { // all-or-nothing linearizable transaction
                select.setLong(1, inId);
                try (ResultSet rs = select.executeQuery()) {
                    if (!rs.next()) {
                        return false;
                    }
                    Phase2 phase2 = readEnum(rs.getString(1), Phase2.class);
                    if (phase2 == Phase2.ABORTING || phase2 == Phase2.ABORTED) {
                        return false;
                    }
                }
            }
            try (var update = connection.prepareStatement("update " + inbox + " set tid = ? where id = ?")) { // Record the tid of this attempt onto the input, bind process and subtransaction id (Inbox[in.id].tid)
                update.setString(1, tid);
                update.setLong(2, inId);
                update.executeUpdate();
            }
            try (var insert = connection.prepareStatement(
                    "insert into " + outbox + " (" + OUTBOX_COLUMNS + ") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) { // insert outbox
                for (Message msg : outputs) {
                    bindMessage(insert, msg);
                    insert.setLong(8, inId);
                    insert.setString(9, tid);
                    insert.setString(10, Delivery.NEW.name());
                    insert.addBatch();
                }
                insert.executeBatch(); // once commit
            }
            return true; //inTransaction submit
        });
    }

    // Atomically records the no-vote state VOTING or VOTED, if the input is present (Figure 3 lines 48-55).
    public void setNoVoteIfPresent(long inId, NoVote state) {
        updateInbox("update " + inbox + " set no_vote = ? where id = ?", state.name(), inId); //Change the input no_vote column to VOTING or VOTED
    }

    // Atomically enters phase 2 unless already terminal and returns the recorded tid; returns null when the input
    // is absent or has no tid (Figure 4 lines 76-82).
    public String enterPhase2(long inId, Decision decision) {
        return inTransaction(connection -> {
            String tid;
            Phase2 phase2;
            try (var select = connection.prepareStatement(
                    "select tid, phase2 from " + inbox + " where id = ? for update")) { // Read the current record's tid and phase-2 status, and lock that row.
                select.setLong(1, inId);
                try (ResultSet rs = select.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    tid = rs.getString(1);
                    phase2 = readEnum(rs.getString(2), Phase2.class);
                }
            }
            // Before reaching the final state, write "COMMITTING" or "ABORTING" as decided; once the final state is reached, do not change it.
            if (phase2 != Phase2.COMMITTED && phase2 != Phase2.ABORTED) {
                Phase2 entering = decision == Decision.COMMIT ? Phase2.COMMITTING : Phase2.ABORTING;
                try (var update = connection.prepareStatement("update " + inbox + " set phase2 = ? where id = ?")) {
                    update.setString(1, entering.name());
                    update.setLong(2, inId);
                    update.executeUpdate();
                }
            }
            return tid; // Regardless of whether the transaction has reached the final state or not, the tid is returned. The caller will then call the "finishTxn" function again.
        });
    }

    // Atomically records COMMITTED or ABORTED after phase-2 handling completes, if present (Figure 4 lines 85-88).
    public void completePhase2IfPresent(long inId, Decision decision) {
        Phase2 terminal = decision == Decision.COMMIT ? Phase2.COMMITTED : Phase2.ABORTED;
        updateInbox("update " + inbox + " set phase2 = ? where id = ?", terminal.name(), inId);
    }

    // Removes a garbage-collected inbox entry (Figure 6 line 112).
    public void removeInbox(long inId) {
        withConnection(connection -> {
            try (var delete = connection.prepareStatement("delete from " + inbox + " where id = ?")) {
                delete.setLong(1, inId);
                delete.executeUpdate();
            }
            return null;
        });
    }

    /*
    Outbox
     */

    // Returns whether any outbox entry still refers to the input, i.e. outputsFrom(Outbox, in.id) != {} (Figure 6
    // line 111).
    public boolean hasOutputsFrom(long inId) {
        return withConnection(connection -> {
            // Search for the output "generated by this input" in the outbox table. Stop as soon as any output is found.
            try (var select = connection.prepareStatement("select 1 from " + outbox + " where in_id = ? limit 1")) {
                select.setLong(1, inId);
                try (ResultSet rs = select.executeQuery()) {
                    return rs.next();
                }
            }
        });
    }

    // Lists retained outputs in NEW delivery state for the outbox poller, ordered by position (Section 3.3).
    public List<OutboxEntry> listNewOutbox() {
        return withConnection(connection -> {
            try (var select = connection.prepareStatement("select " + OUTBOX_COLUMNS
                    + " from " + outbox + " where delivery = ? order by epoch, seq")) {
                select.setString(1, Delivery.NEW.name());
                try (ResultSet rs = select.executeQuery()) {
                    return readOutboxEntries(rs);
                }
            }
        });
    }

    // Lists the distinct targets that have retained outputs, for per-target outbox garbage collection (Section 3.4).
    public List<UUID> listOutboxTargets() {
        return withConnection(connection -> {
            // List out all the UUIDs of the target services that have appeared in the outbox table at present.
            try (var select = connection.prepareStatement("select distinct target from " + outbox);
                    ResultSet rs = select.executeQuery()) {
                List<UUID> targets = new ArrayList<>();
                while (rs.next()) {
                    targets.add(UUID.fromString(rs.getString(1)));
                }
                return targets;
            }
        });
    }

    // Lists one target's retained outputs sorted by position, i.e. sortBy(Outbox, pos) filtered by target (Figure 6 lines 118-119). For GC
    // Read out all the output lines (both NEW and ACKED) that are sent to a certain target from the outbox table, sort them in ascending order by (epoch, seq), and assemble them into a List<OutboxEntry>
    public List<OutboxEntry> listOutboxForTarget(UUID target) {
        return withConnection(connection -> {
            // sortBy(Outbox, pos) + if msg.target = target = where target = ? order by epoch, seq
            try (var select = connection.prepareStatement("select " + OUTBOX_COLUMNS
                    + " from " + outbox + " where target = ? order by epoch, seq")) {
                select.setString(1, target.toString());
                try (ResultSet rs = select.executeQuery()) {
                    return readOutboxEntries(rs);
                }
            }
        });
    }

    // Atomically records ACKED for an output after the target acknowledged durable receipt, if the entry remains
    // (Figure 5 lines 97-99).
    // an output deliver state: NEW -> ACKED
    // ACKED: The poller no longer processes it, and the GC can directly reclaim it.
    public void ackIfPresent(long msgId) {
        withConnection(connection -> {
            try (var update = connection.prepareStatement("update " + outbox + " set delivery = ? where id = ?")) {
                update.setString(1, Delivery.ACKED.name());
                update.setLong(2, msgId);
                update.executeUpdate();
            }
            return null;
        });
    }

    // Removes a garbage-collected outbox entry (Figure 6 lines 133-134).
    public void removeOutbox(long msgId) {
        withConnection(connection -> {
            // Delete an outbox row based on the message ID
            try (var delete = connection.prepareStatement("delete from " + outbox + " where id = ?")) {
                delete.setLong(1, msgId);
                delete.executeUpdate();
            }
            return null;
        });
    }

    /*
    RetireWatermark
     */

    // Reads RetireWatermark[source]; empty means no message from that source has been retired yet.
    public Optional<Position> readWatermark(UUID source) {
        return withConnection(connection -> readWatermark(connection, source));
    }

    // Atomically stores max(RetireWatermark[source], pos) (Figure 6 lines 135-138). The update takes the row's
    // exclusive lock, so it waits for every receive that read the old watermark to commit (see NONE_RETIRED).
    public void advanceWatermark(UUID source, Position pos) {
        inTransaction(connection -> {
            ensureWatermarkRow(connection, source, true);
            try (var update = connection.prepareStatement("update " + watermark + " set epoch = ?, seq = ?"
                    + " where source = ? and (epoch, seq) < (?, ?)")) {
                update.setLong(1, pos.epoch());
                update.setLong(2, pos.seq());
                update.setString(3, source.toString());
                update.setLong(4, pos.epoch());
                update.setLong(5, pos.seq());
                update.executeUpdate();
            }
            return null;
        });
    }

    /*
    Benchmark control (not part of the paper's protocol)
     */

    // Deletes every inbox, outbox, and watermark row; only for resetting state between benchmark runs.
    public void clearMessages() {
        withConnection(connection -> {
            for (String table : new String[]{inbox, outbox, watermark}) {
                try (var stmt = connection.createStatement()) {
                    stmt.execute("delete from " + table);
                }
            }
            return null;
        });
    }

    /*
    Peer identities (implementation detail: addressing other services by their persistent UUIDs, A1)
     */

    // Reads the persisted UUIDs of other services, keyed by the database they host.
    public Map<String, UUID> readPeers() {
        return withConnection(connection -> {
            try (var select = connection.prepareStatement("select db, id from " + peer);
                    ResultSet rs = select.executeQuery()) {
                Map<String, UUID> peers = new HashMap<>();
                while (rs.next()) {
                    peers.put(rs.getString(1), UUID.fromString(rs.getString(2)));
                }
                return peers;
            }
        });
    }

    // Persists the UUID of the service hosting the given database; a service UUID never changes (A1).
    public void savePeer(String db, UUID id) {
        withConnection(connection -> {
            String upsert = dialect == Dialect.MYSQL
                    ? "insert into " + peer + " (db, id) values (?, ?) on duplicate key update id = values(id)"
                    : "insert into " + peer + " (db, id) values (?, ?) on conflict (db) do update set id = excluded.id";
            try (var stmt = connection.prepareStatement(upsert)) {
                stmt.setString(1, db);
                stmt.setString(2, id.toString());
                stmt.executeUpdate();
            }
            return null;
        });
    }

    /*
    Helpers
     */

    // Reads the watermark row of a source on the given connection, without locking it.
    private Optional<Position> readWatermark(Connection connection, UUID source) throws SQLException {
        return readWatermark(connection, source, "");
    }

    // Reads the watermark row of a source under a shared lock held until the transaction ends (MySQL and
    // PostgreSQL both spell it "for share"), so that no advanceWatermark commits before this transaction does.
    private Optional<Position> lockWatermarkShared(Connection connection, UUID source) throws SQLException {
        return readWatermark(connection, source, " for share");
    }

    private Optional<Position> readWatermark(Connection connection, UUID source, String locking)
            throws SQLException {
        try (var select = connection.prepareStatement(
                "select epoch, seq from " + watermark + " where source = ?" + locking)) {
            select.setString(1, source.toString());
            try (ResultSet rs = select.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                Position pos = new Position(rs.getLong(1), rs.getLong(2));
                return pos.compareTo(NONE_RETIRED) <= 0 ? Optional.empty() : Optional.of(pos); // sentinel
            }
        }
    }

    // Creates the source's watermark row with the NONE_RETIRED sentinel if it does not exist yet. On an existing
    // row PostgreSQL's on conflict do nothing takes no lock; MySQL's insert ignore takes a shared lock, which keeps
    // receives of one source concurrent, and its on duplicate key update takes the exclusive lock, so an advance
    // takes that lock here instead of upgrading a shared one (two concurrent advances would otherwise deadlock).
    private void ensureWatermarkRow(Connection connection, UUID source, boolean exclusive) throws SQLException {
        String insert = dialect == Dialect.MYSQL
                ? "insert " + (exclusive ? "" : "ignore ") + "into " + watermark + " (source, epoch, seq)"
                        + " values (?, ?, ?)" + (exclusive ? " on duplicate key update source = source" : "")
                : "insert into " + watermark + " (source, epoch, seq) values (?, ?, ?)"
                        + " on conflict (source) do nothing";
        try (var stmt = connection.prepareStatement(insert)) {
            stmt.setString(1, source.toString());
            stmt.setLong(2, NONE_RETIRED.epoch());
            stmt.setLong(3, NONE_RETIRED.seq());
            stmt.executeUpdate();
        }
    }

    // Runs one conditional single-row inbox update whose where clause implements "if in.id in Inbox".
    private void updateInbox(String sql, String value, long inId) {
        withConnection(connection -> {
            try (var update = connection.prepareStatement(sql)) {
                update.setString(1, value);
                update.setLong(2, inId);
                update.executeUpdate();
            }
            return null;
        });
    }

    // Binds the six immutable message fields to parameters 1-7 (id, gtid, source, target, epoch, seq, body).
    private static void bindMessage(PreparedStatement stmt, Message msg) throws SQLException {
        stmt.setLong(1, msg.id());
        stmt.setString(2, msg.gtid());
        stmt.setString(3, msg.source().toString());
        stmt.setString(4, msg.target().toString());
        stmt.setLong(5, msg.pos().epoch());
        stmt.setLong(6, msg.pos().seq());
        stmt.setBytes(7, msg.body());
    }

    // Reads the six message columns from the current row.
    private static Message readMessage(ResultSet rs) throws SQLException {
        return new Message(rs.getLong("id"), rs.getString("gtid"),
                UUID.fromString(rs.getString("source")), UUID.fromString(rs.getString("target")),
                new Position(rs.getLong("epoch"), rs.getLong("seq")), rs.getBytes("body"));
    }

    // Reads one inbox row.
    private static InboxEntry readInboxEntry(ResultSet rs) throws SQLException {
        return new InboxEntry(readMessage(rs), rs.getString("tid"),
                readEnum(rs.getString("no_vote"), NoVote.class), readEnum(rs.getString("phase2"), Phase2.class));
    }

    // Reads all outbox rows of a result set.
    private static List<OutboxEntry> readOutboxEntries(ResultSet rs) throws SQLException {
        List<OutboxEntry> entries = new ArrayList<>();
        while (rs.next()) {
            entries.add(new OutboxEntry(readMessage(rs), rs.getLong("in_id"),
                    rs.getString("tid"), Delivery.valueOf(rs.getString("delivery"))));
        }
        return entries;
    }

    // Converts a nullable column to an enum constant; null stands for the paper's NULL state.
    private static <E extends Enum<E>> E readEnum(String value, Class<E> type) {
        return value == null ? null : Enum.valueOf(type, value);
    }

    // Runs an individual metadata operation on an auto-commit connection.
    private <T> T withConnection(SqlFunction<T> body) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            return body.apply(connection);
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
    }

    // Runs an atomic block as one all-or-nothing local transaction over the metadata tables.
    private <T> T inTransaction(SqlFunction<T> body) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = body.apply(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new MetadataException(e);
        }
    }

    @FunctionalInterface
    private interface SqlFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    // Signals a failed durable metadata operation.
    public static class MetadataException extends RuntimeException {
        public MetadataException(SQLException cause) {
            super(cause.getMessage(), cause);
        }
    }
}

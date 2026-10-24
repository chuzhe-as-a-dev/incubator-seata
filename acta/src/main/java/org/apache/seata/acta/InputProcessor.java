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

import org.apache.seata.acta.Metadata.EndpointIds;
import org.apache.seata.acta.Metadata.IdLayout;
import org.apache.seata.acta.Metadata.InboxEntry;
import org.apache.seata.acta.Metadata.Message;
import org.apache.seata.acta.Metadata.NoVote;
import org.apache.seata.acta.Metadata.Position;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.rm.datasource.xa.ActaBranchContext;
import org.apache.seata.rm.datasource.xa.ConnectionProxyXA;
import org.apache.seata.rm.datasource.xa.XAXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// Input processing of one service: process, abortAndVote, reservePositions, and settlePositions (Figure 3), plus
// the per-incarnation shared-memory state gEpoch, gNextSeq, and gSettle that startIncarnation initializes
// (Figure 7 lines 139-144). The local transaction of an attempt is a Seata XA branch on the service's Seata XA data
// source: setAutoCommit(false) registers the branch under the input message ID (2PC::register) and starts it,
// close() ends and prepares it (DB::prepareTxn) and reports the yes vote, rollback() aborts it (DB::finishTxn(ABORT)
// of an unprepared attempt); the coordinator's phase-2 decision reaches the prepared branch through Seata's resource
// manager (ActaService.enterPhase2 / completePhase2).
public class InputProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputProcessor.class);

    // Application handler of an activation: runs inside the local transaction and materializes outputs through the
    // activation; returns false on an application-level failure (executeInTxn, Figure 3 line 25).
    @FunctionalInterface
    public interface Handler {
        boolean execute(Activation activation);
    }

    // An output the handler materialized, before it has a position and therefore before it has an ID.
    private record Draft(UUID target, byte[] body) {
    }

    // Context of one activation: its input message, its local transaction, and the outputs materialized so far.
    public final class Activation {
        private final InboxEntry input;
        private final Connection connection;
        private final String tid;
        private final long deadlineNanos;
        private final List<Draft> outputs = new ArrayList<>();

        private Activation(InboxEntry input, Connection connection, String tid, long deadlineNanos) {
            this.input = input;
            this.connection = connection;
            this.tid = tid;
            this.deadlineNanos = deadlineNanos;
        }

        // Returns the input message ID, which is also this activation's participant ID and Seata branch ID.
        public long inId() {
            return input.id();
        }

        // Returns the workflow's global transaction ID.
        public String gtid() {
            return input.message().gtid();
        }

        // Returns the input body, which carries the handler's arguments and continuation state.
        public byte[] body() {
            return input.message().body();
        }

        // Returns the tid of this attempt's local transaction: the text form of its XA branch identifier.
        public String tid() {
            return tid;
        }

        // Returns the connection of this attempt's local transaction (Seata's XA connection proxy), on which the
        // handler executes its data access; the handler must neither commit, roll back, nor close it.
        public Connection connection() {
            return connection;
        }

        // Returns the System.nanoTime() deadline of the unprepared transaction (the branch timeout); the handler's
        // statements should not run past it, like the statement timeouts of the baselines' XA branches.
        public long deadlineNanos() {
            return deadlineNanos;
        }

        // Materializes a remote invocation as an output; it gets its position and ID when the attempt's outputs are
        // positioned, and it is not transmitted until published and prepared (Section 3.1).
        public void invoke(UUID target, byte[] body) {
            outputs.add(new Draft(target, body));
        }

        // Materializes the terminal activation's commit request as a coordinator-directed control output.
        public void requestCommit() {
            invoke(Metadata.COORDINATOR, new byte[0]);
        }
    }

    // gSettle[target]: the settled frontier end, the pending ranges, and the mutex of one target's output stream.
    public static final class Settlement {
        private long end;
        private final Map<Long, Long> ranges = new HashMap<>();

        // Records a reserved range and advances the frontier over every contiguous range (Figure 3 lines 71-74).
        synchronized void settle(long begin, long endExclusive) {
            ranges.put(begin, endExclusive);
            while (ranges.containsKey(end)) {
                end = ranges.remove(end);
            }
        }

        // Returns the settled frontier under the mutex (Figure 6 lines 114-115).
        public synchronized long end() {
            return end;
        }
    }

    // A reserved contiguous sequence range [begin, end) for one target (Figure 3 line 63).
    private record Range(UUID target, long begin, long end) {
    }

    // The positioned output messages of an attempt together with the ranges reserved for them.
    private record Reservation(List<Message> messages, List<Range> ranges) {
    }

    private final MetadataStore store;
    private final DataSource xaDataSource;
    private final ActaCoordinator coordinator;
    private final Handler handler;
    private final IdLayout idLayout;
    private final EndpointIds endpoints;
    private final Duration branchTimeout;
    private volatile long gEpoch;
    private final ConcurrentHashMap<UUID, AtomicLong> gNextSeq = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Settlement> gSettle = new ConcurrentHashMap<>();

    public InputProcessor(MetadataStore store, DataSource xaDataSource, ActaCoordinator coordinator, Handler handler,
            IdLayout idLayout, EndpointIds endpoints, Duration branchTimeout) {
        this.store = store;
        this.xaDataSource = xaDataSource;
        this.coordinator = coordinator;
        this.handler = handler;
        this.idLayout = idLayout;
        this.endpoints = endpoints;
        this.branchTimeout = branchTimeout;
    }

    /*
    Incarnation state (Figure 7 lines 139-144)
     */

    // startIncarnation: durably increments Epoch and resets the volatile per-target maps before workers run. The
    // Epoch is part of every message ID this incarnation produces, so a restarted host never reuses an old ID.
    public void startIncarnation() {
        gEpoch = store.incrementEpoch();
        gNextSeq.clear();
        gSettle.clear();
    }

    // Returns gEpoch of the current incarnation.
    public long epoch() {
        return gEpoch;
    }

    // Returns gSettle[target], creating an entry with end = 0, no ranges, and a fresh mutex when absent.
    public Settlement settlement(UUID target) {
        return gSettle.computeIfAbsent(target, ignored -> new Settlement());
    }

    /*
    Input processing (Figure 3)
     */

    // process(in): registers, executes the activation in a fresh local transaction, publishes its outputs, prepares,
    // and votes (Figure 3 lines 17-46).
    public void process(InboxEntry in) {
        long inId = in.id();
        String gtid = in.message().gtid();
        // The Seata XA data source hands out a branch connection only inside a global transaction, and the branch
        // is registered under the bound input message ID (ActaBranchContext); both are thread-bound for this attempt.
        RootContext.bind(gtid);
        ActaBranchContext.bind(inId);
        try {
            Connection connection = begin(inId);
            if (connection == null) {
                // 2PC::register was rejected: the coordinator already decided abort (Figure 3 lines 19-22).
                store.markAbortedIfPresent(inId);
                return;
            }
            String tid = ((ConnectionProxyXA) connection).getXaTransactionId().toString();
            boolean owned = true;
            try {
                Activation activation = new Activation(in, connection, tid,
                        System.nanoTime() + branchTimeout.toNanos());
                boolean ok;
                try {
                    ok = handler.execute(activation);
                } catch (RuntimeException e) {
                    LOGGER.warn("Activation of input {} failed: {}", inId, e.toString());
                    ok = false;
                }
                if (!ok) {
                    owned = false;
                    abortAndVote(in, connection);
                    return;
                }
                Reservation reservation = reservePositions(gtid, activation.outputs);
                boolean published;
                try {
                    published = store.publishAttempt(inId, tid, reservation.messages());
                } finally {
                    settlePositions(reservation.ranges());
                }
                if (!published) {
                    // The input is already ABORTING/ABORTED or gone: DB::finishTxn(tid, ABORT) (Figure 3 line 41).
                    owned = false;
                    rollback(connection, inId);
                    return;
                }
                owned = false;
                if (!prepare(connection, inId)) {
                    // DB::prepareTxn(tid) refused (Figure 3 lines 43-45): the proxy rolled the branch back and
                    // reported the failure; vote no (lines 47-55, without the local rollback, which is done).
                    store.setNoVoteIfPresent(inId, NoVote.VOTING);
                    coordinator.voteNo(gtid, inId);
                    store.setNoVoteIfPresent(inId, NoVote.VOTED);
                    return;
                }
                // 2PC::voteYes(gtid, inId) (line 46) was reported by the proxy right after the successful prepare.
            } catch (RuntimeException e) {
                if (owned) {
                    rollback(connection, inId);
                }
                throw e;
            }
        } finally {
            ActaBranchContext.unbind();
            RootContext.unbind();
        }
    }

    // DB::beginTxn plus 2PC::register (Figure 3 lines 18 and 24): takes a branch connection of the Seata XA data
    // source, sets the baselines' isolation level (A6: serializable local transactions), and starts the branch,
    // which registers it with the coordinator under the input message ID. Returns null when the coordinator
    // rejected the registration because it already decided abort.
    private Connection begin(long inId) {
        Connection connection;
        try {
            connection = xaDataSource.getConnection();
        } catch (SQLException e) {
            throw new ActaException("beginTxn " + inId, e);
        }
        if (!(connection instanceof ConnectionProxyXA)) {
            close(connection);
            throw new ActaException("beginTxn " + inId + ": the Seata XA data source returned " + connection
                    + " instead of an XA branch connection");
        }
        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            close(connection);
            if (ActaCoordinator.isRejectedRegistration(e)) {
                return null;
            }
            throw new ActaException("register " + inId, e);
        }
    }

    // DB::prepareTxn(tid) (Figure 3 line 43): closing the branch connection ends and prepares the XA branch (with
    // the Sonata operations when enabled) and reports the yes vote; the proxy's SQLException means the prepare was
    // refused or failed and the branch was rolled back, a definitive false.
    private boolean prepare(Connection connection, long inId) {
        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            LOGGER.debug("prepareTxn of input {} refused: {}", inId, e.toString());
            return false;
        }
    }

    // abortAndVote(in, tid): records VOTING, aborts the local transaction, reports a no vote, and records VOTED
    // (Figure 3 lines 47-55).
    private void abortAndVote(InboxEntry in, Connection connection) {
        store.setNoVoteIfPresent(in.id(), NoVote.VOTING);
        rollback(connection, in.id());
        coordinator.voteNo(in.message().gtid(), in.id());
        store.setNoVoteIfPresent(in.id(), NoVote.VOTED);
    }

    // DB::finishTxn(tid, ABORT) of an unprepared attempt: rolls the XA branch back on its own connection (the proxy
    // reports the branch failure to the coordinator) and returns the connection to the pool.
    private static void rollback(Connection connection, long inId) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            // The database rolls an unprepared branch back when its connection is dropped; the proxy already
            // cleared the branch context, so closing below prepares nothing.
            LOGGER.warn("Rolling back the local transaction of input {} failed: {}", inId, e.toString());
        } finally {
            close(connection);
        }
    }

    private static void close(Connection connection) {
        try {
            connection.close();
        } catch (SQLException ignored) {
        }
    }

    // reservePositions(msgs): groups outputs by target, reserves one contiguous range per target with an atomic
    // fetch-and-add on gNextSeq[target], and assigns each output a position {gEpoch, seq} (Figure 3 lines 56-67).
    // The position completes the output's identity, so its message ID pack(source, target, epoch, seq) is built here.
    private Reservation reservePositions(String gtid, List<Draft> outputs) {
        Map<UUID, List<Draft>> groups = new LinkedHashMap<>();
        for (Draft draft : outputs) {
            groups.computeIfAbsent(draft.target(), ignored -> new ArrayList<>()).add(draft);
        }
        List<Range> ranges = new ArrayList<>();
        List<Message> positioned = new ArrayList<>(outputs.size());
        long epoch = gEpoch;
        UUID source = store.serviceId();
        int sourceEndpoint = endpoints.of(source);
        for (Map.Entry<UUID, List<Draft>> group : groups.entrySet()) {
            UUID target = group.getKey();
            int targetEndpoint = endpoints.of(target);
            int size = group.getValue().size();
            long seq = gNextSeq.computeIfAbsent(target, ignored -> new AtomicLong()).getAndAdd(size);
            ranges.add(new Range(target, seq, seq + size)); // [seq, seq+size)
            for (Draft draft : group.getValue()) {
                positioned.add(new Message(idLayout.pack(sourceEndpoint, targetEndpoint, epoch, seq), gtid, source,
                        target, new Position(epoch, seq), draft.body()));
                seq += 1;
            }
        }
        return new Reservation(positioned, ranges);
    }

    // settlePositions(ranges): records every reserved range under its target's mutex and advances the settled
    // frontier (Figure 3 lines 68-74).
    private void settlePositions(List<Range> ranges) {
        for (Range range : ranges) {
            settlement(range.target()).settle(range.begin(), range.end());
        }
    }
}

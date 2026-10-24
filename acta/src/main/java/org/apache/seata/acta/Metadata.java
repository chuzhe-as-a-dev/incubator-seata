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

import java.util.UUID;

// Durable metadata entry types of an Acta service (paper Figure 2, Section 3.2).
public final class Metadata {

    // Target identifier of the commit-request control output that is directed to the coordinator.
    public static final UUID COORDINATOR = new UUID(0L, 0L);

    // Source identifier of root inputs, which come from the external client rather than a service stream (A.1).
    public static final UUID CLIENT = new UUID(0L, 1L);

    // Reserved compact endpoint numbers of the coordinator and the client in message IDs; services get their numbers
    // from the deployment configuration, starting at FIRST_SERVICE_ENDPOINT.
    public static final int COORDINATOR_ENDPOINT = 0;
    public static final int CLIENT_ENDPOINT = 1;
    public static final int FIRST_SERVICE_ENDPOINT = 2;

    private Metadata() {
    }

    // Stream position assigned by a producing attempt; compares lexicographically by epoch, then sequence number.
    public record Position(long epoch, long seq) implements Comparable<Position> {
        @Override
        public int compareTo(Position other) {
            int byEpoch = Long.compare(epoch, other.epoch);
            return byEpoch != 0 ? byEpoch : Long.compare(seq, other.seq);
        }
    }

    // Compact endpoint numbers of the services a process talks to, for message IDs; stable and unique per
    // deployment (A2 needs globally unique message IDs, and the per-target sequence numbers are only unique per
    // (source, target, epoch)).
    @FunctionalInterface
    public interface EndpointIds {
        int of(UUID service);
    }

    // Message IDs are the message's identity (source, target, epoch, seq) packed losslessly into the 63 value bits
    // of a positive long: the sign bit stays 0, then the source endpoint, the target endpoint, the epoch, and the
    // per-target sequence number, each in a fixed number of bits. A field that does not fit its bits is an error,
    // never truncated or wrapped. The layout is deployment configuration and must be the same on every host.
    public record IdLayout(int sourceBits, int targetBits, int epochBits) {
        public IdLayout {
            if (sourceBits < 1 || targetBits < 1 || epochBits < 1 || sourceBits + targetBits + epochBits > 62) {
                throw new IllegalArgumentException("Invalid message ID layout " + sourceBits + "/" + targetBits + "/"
                        + epochBits + " bits: each field needs at least 1 bit and the sequence number too");
            }
        }

        // Bits left for the sequence number.
        public int seqBits() {
            return 63 - sourceBits - targetBits - epochBits;
        }

        // Packs the identity into a message ID; throws if a field exceeds its width.
        public long pack(int source, int target, long epoch, long seq) {
            return (check(source, sourceBits, "source") << (targetBits + epochBits + seqBits()))
                    | (check(target, targetBits, "target") << (epochBits + seqBits()))
                    | (check(epoch, epochBits, "epoch") << seqBits())
                    | check(seq, seqBits(), "seq");
        }

        private static long check(long value, int bits, String field) {
            if (value < 0 || value >= (1L << bits)) {
                throw new IllegalStateException("Message ID field " + field + " = " + value + " does not fit in "
                        + bits + " bits");
            }
            return value;
        }
    }

    // No-vote state of an inbox entry
    public enum NoVote {VOTING, VOTED}

    // Phase-2 state of an inbox entry
    public enum Phase2 {COMMITTING, ABORTING, COMMITTED, ABORTED}

    // Delivery state of an outbox entry.
    public enum Delivery {NEW, ACKED}

    // Global decision applied by finish(inId, decision) and DB::finishTxn(tid, decision) (Figure 4).
    public enum Decision {COMMIT, ABORT}

    // Immutable service message <id, gtid, src, dst, pos, body>; id is the packed identity (IdLayout), gtid is the
    // Seata xid string, source and target are the services' persistent UUIDs.
    public record Message(long id, String gtid, UUID source, UUID target, Position pos, byte[] body) {
    }

    // Inbox entry: the input message plus the latest attempt's tid (the XA branch identifier as text, see
    // XaBranches), its no-vote state, and its phase-2 state.
    public record InboxEntry(Message message, String tid, NoVote noVote, Phase2 phase2) {
        // Returns the message ID that identifies this entry.
        public long id() {
            return message.id();
        }
    }

    // Outbox entry: the output message plus the producing input's ID, the producing attempt's tid, and delivery state.
    public record OutboxEntry(Message message, long in, String tid, Delivery delivery) {
        // Returns the message ID that identifies this entry.
        public long id() {
            return message.id();
        }
    }

    // Retire watermark entry: the position through which all messages from a source have been retired.
    public record RetireWatermarkEntry(UUID source, Position pos) {
    }
}

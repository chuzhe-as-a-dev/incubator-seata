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

import org.apache.seata.acta.Metadata.Delivery;
import org.apache.seata.acta.Metadata.InboxEntry;
import org.apache.seata.acta.Metadata.Message;
import org.apache.seata.acta.Metadata.OutboxEntry;
import org.apache.seata.acta.Metadata.Position;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.UUID;

// Message delivery and receiving of one service (Figure 5): the sender-side deliver procedure driven by the poller,
// and the target-side receive operation that durably inserts or deduplicates an incoming message.
public class MessageDelivery {

    // Remote procedure calls to another service: a call returns only after the target durably applied it; a call
    // that throws has an unknown outcome and is retried by the caller's next poll or collection round (A7).
    public interface Transport {
        // RPC::receive(target, msg) (Figure 5 line 96).
        void receive(UUID target, Message msg);

        // RPC::advanceWatermark(target, source, pos) (Figure 6 line 132).
        void advanceWatermark(UUID target, UUID source, Position pos);
    }

    private static final Duration SPIN_DELAY = Duration.ofMillis(1);

    private final MetadataStore store;
    private final XaBranches xa;
    private final ActaCoordinator coordinator;
    private final Transport transport;
    private Consumer<InboxEntry> inboxChanged = in -> {
    };

    public MessageDelivery(MetadataStore store, XaBranches xa, ActaCoordinator coordinator, Transport transport) {
        this.store = store;
        this.xa = xa;
        this.coordinator = coordinator;
        this.transport = transport;
    }

    // Sets the hook that receives every accepted input right after it is durable: the inbox dispatcher's
    // receive -> process hand-off (Section 3.3); the Inbox is not polled.
    public void onInboxChanged(Consumer<InboxEntry> hook) {
        this.inboxChanged = hook;
    }

    /*
    Sender side (Figure 5 lines 89-100)
     */

    // deliver(msg): transmits a NEW output once its producing attempt is prepared, unless the input's current state
    // shows the output must not or need not be transmitted; records ACKED after the target's durable receipt.
    public void deliver(OutboxEntry msg) {
        while (msg.delivery() == Delivery.NEW) {
            Optional<InboxEntry> in = store.readInbox(msg.in());
            if (in.isEmpty() || in.get().noVote() != null || in.get().phase2() != null
                    || !msg.tid().equals(in.get().tid())) {
                return;
            }
            if (xa.isPrepared(msg.tid())) {
                if (Metadata.COORDINATOR.equals(msg.message().target())) {
                    coordinator.requestCommit(msg.message().gtid());
                } else {
                    transport.receive(msg.message().target(), msg.message());
                }
                store.ackIfPresent(msg.id()); // ACKED only after the target durably holds the message.
                return;
            }
            pause();
        }
    }

    // Waits briefly before re-checking an output whose producing attempt has not prepared yet.
    private static void pause() {
        try {
            Thread.sleep(SPIN_DELAY.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for prepare", e);
        }
    }

    /*
    Target side (Figure 5 lines 101-106)
     */

    // receive(msg): durably inserts or deduplicates the message, then hands a newly inserted input to a worker for
    // its first (and only) dispatch. Returns false when nothing was inserted: a duplicate or a retired position.
    public boolean receive(Message msg) {
        if (!store.receive(msg)) { // watermark check + deduplication by ID + insert in the NULL state
            return false;
        }
        inboxChanged.accept(new InboxEntry(msg, null, null, null));
        return true;
    }
}

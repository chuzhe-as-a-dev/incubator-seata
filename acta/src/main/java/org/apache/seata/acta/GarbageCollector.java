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
import org.apache.seata.acta.Metadata.NoVote;
import org.apache.seata.acta.Metadata.OutboxEntry;
import org.apache.seata.acta.Metadata.Phase2;
import org.apache.seata.acta.Metadata.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

// Watermark-based garbage collection of one service's inbox and outbox entries (Section 3.4, Figure 6): the poller
// periodically hands collectOnce to a worker, which invokes tryGcOutbox for each target and tryGcInbox for each
// durable inbox entry.
public class GarbageCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(GarbageCollector.class);

    private final MetadataStore store;
    private final InputProcessor processor;
    private final MessageDelivery.Transport transport;

    public GarbageCollector(MetadataStore store, InputProcessor processor, MessageDelivery.Transport transport) {
        this.store = store;
        this.processor = processor;
        this.transport = transport;
    }

    // One collection round: every target's outbox stream, then every durable inbox entry (Section 3.4).
    public void collectOnce() {
        try {
            for (UUID target : store.listOutboxTargets()) {
                try {
                    tryGcOutbox(target);
                } catch (RuntimeException e) {
                    LOGGER.warn("Outbox collection for target {} failed, will retry next round: {}", target, e.toString());
                }
            }
            for (InboxEntry in : store.listInbox()) {
                tryGcInbox(in);
            }
        } catch (RuntimeException e) {
            LOGGER.warn("Garbage collection round failed, will retry", e);
        }
    }

    // tryGcInbox(in): removes the entry once its source retired its position, local processing completed, and no
    // retained output refers to it (Figure 6 lines 107-112). The watermark read here needs no lock: it is a
    // committed value, every receive that read an older one committed before it (MetadataStore.NONE_RETIRED), and
    // every later receive of this message sees it, so the deletion cannot be undone by a late duplicate.
    void tryGcInbox(InboxEntry in) {
        Optional<Position> watermark = store.readWatermark(in.message().source()); //readWatermark(source) exists
        boolean retired = watermark.isPresent() && in.message().pos().compareTo(watermark.get()) <= 0; // pos <= RetireWatermark
        boolean completed = in.noVote() == NoVote.VOTED
                || in.phase2() == Phase2.COMMITTED || in.phase2() == Phase2.ABORTED;
        if (retired && completed && !store.hasOutputsFrom(in.id())) {
            store.removeInbox(in.id());
        }
    }

    // tryGcOutbox(target): scans the target's outputs in position order up to the settled frontier, stops at a NEW
    // output that may still need transmission, advances the target's watermark, then removes the collected
    // entries (Figure 6 lines 113-134).
    void tryGcOutbox(UUID target) {
        long settledEnd = processor.settlement(target).end(); // Reading from the lock of Settlement
        long epoch = processor.epoch();
        Position end = null;
        List<Long> gcIds = new ArrayList<>();
        for (OutboxEntry msg : store.listOutboxForTarget(target)) {
            Position pos = msg.message().pos();
            if (pos.epoch() == epoch && pos.seq() >= settledEnd) {
                break;
            }
            if (msg.delivery() == Delivery.NEW && mayStillRequireTransmission(msg)) {
                break;
            }
            end = pos;
            gcIds.add(msg.id());
        }
        if (end == null) {
            return;
        }
        if (!Metadata.COORDINATOR.equals(target)) {
            transport.advanceWatermark(target, store.serviceId(), end);
        }
        for (long id : gcIds) {
            store.removeOutbox(id);
        }
    }

    // The same validation as deliver: a NEW output still requires transmission only while its input is present,
    // has no no-vote or phase-2 state, and still records the output's tid (Figure 6 lines 124-127).
    private boolean mayStillRequireTransmission(OutboxEntry msg) {
        Optional<InboxEntry> in = store.readInbox(msg.in());
        return in.isPresent() && in.get().noVote() == null && in.get().phase2() == null
                && msg.tid().equals(in.get().tid());
    }

    // advanceWatermark(source, pos): the target-side RPC handler that stores the maximum (Figure 6 lines 135-138).
    public void advanceWatermark(UUID source, Position pos) {
        store.advanceWatermark(source, pos);
    }
}

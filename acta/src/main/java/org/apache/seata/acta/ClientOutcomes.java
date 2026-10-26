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
import org.apache.seata.acta.Metadata.InboxEntry;
import org.apache.seata.acta.Metadata.Phase2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

// Withheld client output of root inputs (Appendix A.1, Assumption A8): once a root input's global decision has been
// applied, its outcome is sent asynchronously to the external client; the root input row is the durable client
// result and is removed only after the client acknowledges it, so a restarted root resends pending outcomes.
public class ClientOutcomes {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientOutcomes.class);

    // Delivers one outcome to the client; returns only after the client recorded it, throws otherwise.
    @FunctionalInterface
    public interface Sink {
        void deliver(long rootId, Decision decision, byte[] body);
    }

    private final MetadataStore store;
    private final Sink sink;
    private final Duration retryDelay;

    public ClientOutcomes(MetadataStore store, Sink sink, Duration retryDelay) {
        this.store = store;
        this.sink = sink;
        this.retryDelay = retryDelay;
    }

    // Delay before a send the client did not acknowledge is retried.
    public Duration retryDelay() {
        return retryDelay;
    }

    // Lists the decided root inputs whose outcome still has to be sent: run once at start for the roots an earlier
    // incarnation decided but could not deliver (A8). While running, completePhase2 hands decided roots over
    // directly, so this is not polled.
    public List<InboxEntry> pending() {
        return store.listDecidedRoots();
    }

    // Sends the outcome of a decided root input, then removes the root input, which is no longer needed (A8).
    // Reads the input's current row: an input that is not a decided root (another kind of input, not decided yet,
    // or already removed) needs nothing. Returns false only when the send failed and must be retried.
    public boolean send(long rootId) {
        Optional<InboxEntry> row = store.readInbox(rootId);
        if (row.isEmpty() || !Metadata.CLIENT.equals(row.get().message().source())
                || (row.get().phase2() != Phase2.COMMITTED && row.get().phase2() != Phase2.ABORTED)) {
            return true;
        }
        InboxEntry root = row.get();
        try {
            Decision decision = root.phase2() == Phase2.COMMITTED ? Decision.COMMIT : Decision.ABORT;
            sink.deliver(root.id(), decision, root.message().body());
            store.removeInbox(root.id());
            return true;
        } catch (RuntimeException e) {
            LOGGER.warn("Sending outcome of root input {} failed, will retry: {}", root.id(), e.toString());
            return false;
        }
    }
}

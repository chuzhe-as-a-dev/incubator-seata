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
    private volatile long retryAfterNanos;

    public ClientOutcomes(MetadataStore store, Sink sink, Duration retryDelay) {
        this.store = store;
        this.sink = sink;
        this.retryDelay = retryDelay;
    }

    // Lists decided root inputs whose outcome still has to be sent; empty while backing off after a failed send.
    public List<InboxEntry> pending() {
        if (System.nanoTime() < retryAfterNanos) {
            return List.of();
        }
        return store.listDecidedRoots();
    }

    // Sends the outcome, then removes the root input, which is no longer needed (A8).
    public void send(InboxEntry root) {
        try {
            Decision decision = root.phase2() == Phase2.COMMITTED ? Decision.COMMIT : Decision.ABORT;
            sink.deliver(root.id(), decision, root.message().body());
            store.removeInbox(root.id());
        } catch (RuntimeException e) {
            retryAfterNanos = System.nanoTime() + retryDelay.toNanos();
            LOGGER.warn("Sending outcome of root input {} failed, will retry: {}", root.id(), e.toString());
        }
    }
}

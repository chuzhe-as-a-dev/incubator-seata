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
import org.apache.seata.acta.Metadata.EndpointIds;
import org.apache.seata.acta.Metadata.Message;
import org.apache.seata.acta.Metadata.Position;
import org.apache.seata.acta.MetadataStore.Dialect;
import org.apache.seata.rm.datasource.xa.ActaPhase2Hook;
import org.apache.seata.rm.datasource.xa.DataSourceProxyXA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.time.Duration;
import java.util.List;

// One Acta service (Section 3.1) built into Seata: wires the runtime components of a service with its own local
// database and runs the startup order of Section 3.5 (new incarnation, then the poller that drives process,
// deliver, and gc). The service's business transactions are Seata XA branches on its DataSourceProxyXA; its
// metadata lives in the same database, accessed through the plain data source. Phase-2 decisions of its branches
// arrive from Seata's XA resource manager through the ActaPhase2Hook this service registers (Figure 4).
// Failure recovery (Section 3.5 beyond the Epoch increment) is not implemented in this version.
public class ActaService implements ActaPhase2Hook {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActaService.class);

    // Worker settings of one service; branchTimeout bounds an unprepared attempt like the baselines' XA branches.
    public record Settings(Duration pollInterval, int workerThreads, Duration gcInterval, Duration branchTimeout) {
    }

    private final MetadataStore store;
    private final XaBranches xa;
    private final String resourceId;
    private final ActaCoordinator coordinator;
    private final InputProcessor processor;
    private final MessageDelivery delivery;
    private final GarbageCollector gc;
    private final Poller poller;

    public ActaService(DataSource metadataDataSource, DataSourceProxyXA xaDataSource, Dialect dialect,
            InputProcessor.Handler handler, MessageDelivery.Transport transport, ClientOutcomes.Sink clientSink,
            EndpointIds endpoints, Settings settings) {
        if (!ActaConfig.enabled()) {
            throw new IllegalStateException("ACTA is disabled in this process (seata.acta.enable)");
        }
        this.store = new MetadataStore(metadataDataSource, dialect, ActaConfig.tablePrefix());
        this.xa = new XaBranches(metadataDataSource, dialect);
        this.resourceId = xaDataSource.getResourceId();
        this.coordinator = new ActaCoordinator();
        this.processor = new InputProcessor(store, xaDataSource, coordinator, handler, ActaConfig.idLayout(),
                endpoints, settings.branchTimeout());
        this.delivery = new MessageDelivery(store, xa, coordinator, transport);
        this.gc = new GarbageCollector(store, processor, transport);
        ClientOutcomes outcomes = new ClientOutcomes(store, clientSink, settings.gcInterval());
        this.poller = new Poller(store, processor, delivery, gc, outcomes, settings.pollInterval(),
                settings.gcInterval(), settings.workerThreads());
        delivery.onInboxChanged(poller::wakeUp);
    }

    // Returns the metadata store, e.g. for the service's persistent UUID.
    public MetadataStore store() {
        return store;
    }

    // Returns the delivery component whose receive is exposed as the service's RPC endpoint.
    public MessageDelivery delivery() {
        return delivery;
    }

    // Returns the garbage collector whose advanceWatermark is exposed as the service's RPC endpoint.
    public GarbageCollector gc() {
        return gc;
    }

    // Root admission (A.1): durably accepts a client's root input under a fresh global transaction and returns once
    // it is in the Inbox; a resubmission of the same root ID is deduplicated. The client allocates the root's ID and
    // position (its own endpoint, this service as target, its persisted epoch, and its sequence number) by the same
    // rule as a service's outputs, and a resubmission carries the same ID and position.
    public void admit(long rootId, Position pos, byte[] body) {
        if (store.readInbox(rootId).isPresent()) {
            return;
        }
        String gtid = coordinator.begin();
        delivery.receive(new Message(rootId, gtid, Metadata.CLIENT, store.serviceId(), pos, body));
    }

    // Starts the service: initialize metadata, make this service's phase-2 operations reachable from the resource
    // manager, start a new incarnation, then start the workers (Section 3.5). Inputs retained from an earlier
    // incarnation that no attempt has published are dispatched by the workers like any pending input; inputs with
    // a recorded attempt are left as they are (no recovery in this version).
    public void start() {
        store.initialize();
        ActaPhase2Hook.Registry.register(resourceId, this);
        processor.startIncarnation();
        poller.start(List.of());
        LOGGER.info("Acta service {} (resource {}) started in epoch {}", store.serviceId(), resourceId,
                processor.epoch());
    }

    // Stops the poller and its workers.
    public void stop() {
        poller.stop();
    }

    // Benchmark control: stops the workers, rolls back every prepared transaction, wipes the message metadata,
    // and starts a fresh incarnation; must only be called between runs.
    public int resetForBenchmark() {
        poller.stop();
        int rolledBack = xa.rollbackAllPrepared();
        store.clearMessages();
        start();
        return rolledBack;
    }

    /*
    Phase 2 (Figure 4): finish(inId, decision), run by Seata's XA resource manager around the branch's XA finish.
     */

    // Records COMMITTING/ABORTING and tells whether an attempt recorded its local transaction (Figure 4 lines 76-83).
    @Override
    public boolean enterPhase2(long inId, boolean commit) {
        return store.enterPhase2(inId, commit ? Decision.COMMIT : Decision.ABORT) != null;
    }

    // Records COMMITTED/ABORTED (Figure 4 lines 85-88); a decision may release a withheld client outcome, so it
    // wakes the poller (A8).
    @Override
    public void completePhase2(long inId, boolean commit) {
        store.completePhase2IfPresent(inId, commit ? Decision.COMMIT : Decision.ABORT);
        poller.wakeUp();
    }
}

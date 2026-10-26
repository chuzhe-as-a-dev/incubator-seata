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

import org.apache.seata.acta.Metadata.InboxEntry;
import org.apache.seata.acta.Metadata.OutboxEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

// The workers of one service and the one poller the paper has, the outbox poller (Section 3.3). Inputs are not
// polled: receive hands an accepted input straight to a worker for process (Section 3.3: receive -> process), and
// completePhase2 hands a decided root input straight to a worker for its client outcome (A8). Only the outbox is
// scanned, every pollInterval and at once after a prepare (wakeUp), because deliver must be retried until the target
// has acknowledged the output (Figure 5). Garbage collection runs on its own timer (Section 3.4). The one-off scans
// at start cover the pending inputs and the decided roots an earlier incarnation left behind (Section 3.5). A task
// never runs twice concurrently for the same input or output (the claimed sets), and an input is processed at most
// once per incarnation (A5).
public class Poller {

    private static final Logger LOGGER = LoggerFactory.getLogger(Poller.class);

    private final MetadataStore store;
    private final InputProcessor processor;
    private final MessageDelivery delivery;
    private final GarbageCollector gc;
    private final ClientOutcomes outcomes;
    private final Duration pollInterval;
    private final Duration gcInterval;
    private final int workerThreads;

    private final Set<Long> dispatched = ConcurrentHashMap.newKeySet(); // inputs of this incarnation being processed
    private final Set<Long> delivering = ConcurrentHashMap.newKeySet();
    private final Set<Long> sending = ConcurrentHashMap.newKeySet();
    private volatile boolean running;
    private volatile boolean outboxDirty;
    private ExecutorService workers;
    private ScheduledExecutorService timer;
    private Thread outboxPoller;

    public Poller(MetadataStore store, InputProcessor processor, MessageDelivery delivery, GarbageCollector gc,
            ClientOutcomes outcomes, Duration pollInterval, Duration gcInterval, int workerThreads) {
        this.store = store;
        this.processor = processor;
        this.delivery = delivery;
        this.gc = gc;
        this.outcomes = outcomes;
        this.pollInterval = pollInterval;
        this.gcInterval = gcInterval;
        this.workerThreads = workerThreads;
    }

    // Starts the workers, the outbox poller, and the garbage collection timer, and queues what the previous
    // incarnation left behind: inputs no attempt has published yet (processed like any new input) and root inputs
    // whose decision is applied but whose outcome the client has not acknowledged (resent, A8).
    public void start(Collection<InboxEntry> pendingInputs, Collection<InboxEntry> decidedRoots) {
        dispatched.clear();
        delivering.clear();
        sending.clear();
        running = true;
        workers = Executors.newFixedThreadPool(workerThreads, named("acta-worker-"));
        timer = Executors.newSingleThreadScheduledExecutor(named("acta-timer-"));
        timer.scheduleWithFixedDelay(this::collect, gcInterval.toMillis(), gcInterval.toMillis(),
                TimeUnit.MILLISECONDS);
        outboxPoller = Thread.ofPlatform().name("acta-outbox-poller").daemon().start(this::pollOutbox);
        for (InboxEntry in : pendingInputs) {
            dispatch(in);
        }
        for (InboxEntry root : decidedRoots) {
            sendOutcome(root.id());
        }
    }

    // Stops the poller, the timer, and the workers, waiting briefly for running tasks.
    public void stop() {
        running = false;
        if (outboxPoller != null) {
            outboxPoller.interrupt();
        }
        if (timer != null) {
            timer.shutdownNow();
        }
        if (workers != null) {
            workers.shutdownNow();
            try {
                workers.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /*
    Inputs: receive -> process (Section 3.3, Assumption A5)
     */

    // Hands a pending input to a worker for process; an input already claimed in this incarnation is left alone.
    // Called by receive right after the input is durable, and at start for the inputs of an earlier incarnation.
    public void dispatch(InboxEntry in) {
        if (!running || !isPending(in) || !dispatched.add(in.id())) {
            return;
        }
        try {
            workers.execute(() -> runProcess(in));
        } catch (RejectedExecutionException e) {
            dispatched.remove(in.id());
        }
    }

    // Fig 3: process. An infrastructure failure leaves the input in the Inbox for the next incarnation (A5; no
    // recovery yet), and a successful attempt may have produced outputs: poll the outbox now.
    private void runProcess(InboxEntry in) {
        try {
            processor.process(in);
        } catch (RuntimeException e) {
            LOGGER.error("process of input {} failed; it stays in the Inbox until the next incarnation", in.id(), e);
        } finally {
            dispatched.remove(in.id());
        }
        wakeUp();
    }

    // Returns whether no attempt of the input has been published, voted no, or entered phase 2.
    private static boolean isPending(InboxEntry in) {
        return in.tid() == null && in.noVote() == null && in.phase2() == null;
    }

    /*
    Outbox poller -> deliver (Section 3.3)
     */

    // Makes the outbox poller scan now instead of at the end of its current interval.
    public void wakeUp() {
        outboxDirty = true;
        Thread poller = outboxPoller;
        if (poller != null) {
            LockSupport.unpark(poller);
        }
    }

    // The outbox poller: scans the NEW outputs and hands each unclaimed one to a worker for deliver, then sleeps
    // for pollInterval unless woken up. The scan is the retry mechanism of deliver (Figure 5): an output stays NEW
    // until its target durably received it, so a failed or refused delivery is simply found again.
    private void pollOutbox() {
        while (running) {
            outboxDirty = false;
            try {
                for (OutboxEntry msg : store.listNewOutbox()) {
                    if (delivering.add(msg.id())) {
                        try {
                            workers.execute(() -> runDeliver(msg));
                        } catch (RejectedExecutionException e) {
                            delivering.remove(msg.id());
                        }
                    }
                }
            } catch (RuntimeException e) {
                LOGGER.warn("Outbox scan failed, will retry", e);
            }
            if (!outboxDirty && running) {
                LockSupport.parkNanos(pollInterval.toNanos());
            }
        }
    }

    private void runDeliver(OutboxEntry msg) {
        try {
            delivery.deliver(msg);
        } catch (RuntimeException e) {
            LOGGER.debug("deliver of output {} failed, will retry on a later poll: {}", msg.id(), e.toString());
        } finally {
            delivering.remove(msg.id());
        }
    }

    /*
    Client outcomes (Assumption A8)
     */

    // Hands a decided root input to a worker that sends its outcome to the client; non-root inputs and inputs that
    // are not decided are ignored by the send. Called by completePhase2 and at start for the leftover roots.
    public void sendOutcome(long rootId) {
        if (!running || !sending.add(rootId)) {
            return;
        }
        try {
            workers.execute(() -> runOutcome(rootId));
        } catch (RejectedExecutionException e) {
            sending.remove(rootId);
        }
    }

    // A send the client did not acknowledge is retried after the outcome retry delay.
    private void runOutcome(long rootId) {
        boolean done;
        try {
            done = outcomes.send(rootId);
        } finally {
            sending.remove(rootId);
        }
        if (!done && running) {
            timer.schedule(() -> sendOutcome(rootId), outcomes.retryDelay().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /*
    Garbage collection (Section 3.4)
     */

    private void collect() {
        try {
            gc.collectOnce();
        } catch (RuntimeException e) {
            LOGGER.warn("Garbage collection round failed, will retry", e);
        }
    }

    private static ThreadFactory named(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, prefix + n.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }
}

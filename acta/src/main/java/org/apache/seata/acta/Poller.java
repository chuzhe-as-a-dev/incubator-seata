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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;

// The workers of one service. Every worker runs the same loop: in each round it checks whether an inbox input waits
// for process (inbox dispatcher, Section 3.3), whether an outbox output waits for deliver (outbox poller,
// Section 3.3), whether a decided root input's outcome waits to be sent (A8), and whether garbage collection is due
// (Section 3.4), and does at most one of each. Deliver therefore never queues behind a backlog of process work; when
// every worker is blocked on database locks, the lock and transaction timeouts free them. A task never runs twice
// concurrently for the same input or output (Section 3.1), and each input is processed at most once per incarnation
// (Assumption A5).
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

    private final Set<Long> dispatched = ConcurrentHashMap.newKeySet(); // "dispatched" is the collection of this incarnation.
    private final Set<Long> delivering = ConcurrentHashMap.newKeySet();
    private final Set<Long> sending = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<InboxEntry> recovered = new ConcurrentLinkedQueue<>();
    private final Source<InboxEntry> inbox;
    private final Source<OutboxEntry> outbox;
    private final Source<InboxEntry> decidedRoots;
    private final AtomicBoolean collecting = new AtomicBoolean();
    private volatile long nextCollectionAtNanos;
    private volatile boolean running;
    private final List<Thread> workers = new ArrayList<>();
    private final ConcurrentLinkedDeque<Thread> idle = new ConcurrentLinkedDeque<>();

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
        this.inbox = new Source<>("Inbox", store::listPendingInbox, InboxEntry::id, dispatched);
        this.outbox = new Source<>("Outbox", store::listNewOutbox, OutboxEntry::id, delivering);
        this.decidedRoots = new Source<>("Client outcome", outcomes::pending, InboxEntry::id, sending);
    }

    // Starts after recovery classified every retained input: queues the inputs recovery selected for redispatch, then
    // starts the workers; inputs accepted meanwhile wait in the durable Inbox for their first dispatch (Section 3.5).
    public void start(Collection<InboxEntry> toDispatch) {
        workers.clear();
        dispatched.clear();
        delivering.clear();
        sending.clear();
        recovered.clear();
        for (InboxEntry in : toDispatch) {
            dispatched.add(in.id());
            recovered.add(in);
        }
        nextCollectionAtNanos = System.nanoTime() + gcInterval.toNanos();
        running = true;
        for (int i = 0; i < workerThreads; i++) {
            workers.add(Thread.ofPlatform().name("acta-worker-" + i).daemon().start(this::loop)); // Restart the dispatcher
        }
    }

    // Stops the workers and waits briefly for them to exit.
    public void stop() {
        running = false;
        workers.forEach(Thread::interrupt);
        for (Thread worker : workers) {
            try {
                worker.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    // Signals new work, e.g. an input was received, outputs became deliverable, or a decision was applied: the next
    // scans happen now and one idle worker wakes up.
    public void wakeUp() {
        inbox.markDirty();
        outbox.markDirty();
        decidedRoots.markDirty();
        Thread worker = idle.pollFirst();
        if (worker != null) {
            LockSupport.unpark(worker);
        }
    }

    // The worker loop of the whiteboard: while true { if an input exists: process; if an output exists: deliver;
    // if an outcome exists: send; if gc is needed: collect }, idling for pollInterval when there was nothing to do.
    private void loop() {
        while (running) {
            boolean worked = processOne() | deliverOne() | sendOneOutcome() | collectIfDue();
            if (!worked && running) {
                Thread self = Thread.currentThread();
                idle.addLast(self);
                LockSupport.parkNanos(pollInterval.toNanos());
                idle.remove(self);
            }
        }
    }

    /*
    Inbox dispatcher (Section 3.3, Assumption A5)
     */

    // Processes one input: first an input that recovery selected, otherwise a pending input from the Inbox.
    private boolean processOne() {
        InboxEntry retained = recovered.poll();
        if (retained != null) {
            runProcess(retained);
            return true;
        }
        InboxEntry candidate = inbox.next();
        if (candidate == null) {
            return false;
        }
        dispatch(candidate.id());
        return true;
    }

    // Rereads the input and processes it only if it is still pending; otherwise releases it.
    // readInbox is executed first and then the processing is carried out, rather than directly using the message object when calling receive.
    private void dispatch(long inId) {
        Optional<InboxEntry> current;
        try {
            current = store.readInbox(inId);
        } catch (RuntimeException e) {
            dispatched.remove(inId);
            LOGGER.warn("Reading input {} failed, will retry on a later poll: {}", inId, e.toString());
            return;
        }
        if (current.isEmpty() || !isPending(current.get())) {
            dispatched.remove(inId);
            return;
        }
        runProcess(current.get());
    }

    // Runs process for one input; an infrastructure failure leaves the input in the Inbox (no recovery yet, A5).
    // Fig 3: process
    private void runProcess(InboxEntry in) {
        try {
            processor.process(in);
            dispatched.remove(in.id());
            wakeUp();
        } catch (RuntimeException e) {
            LOGGER.error("process of input {} failed; it stays in the Inbox", in.id(), e);
        }
    }

    // Returns whether no attempt of the input has been published, voted no, or entered phase 2.
    private static boolean isPending(InboxEntry in) {
        return in.tid() == null && in.noVote() == null && in.phase2() == null;
    }

    /*
    Outbox poller (Section 3.3)
     */

    // Delivers one NEW output that no other worker is delivering. -> deliver
    private boolean deliverOne() {
        OutboxEntry msg = outbox.next();
        if (msg == null) {
            return false;
        }
        try {
            delivery.deliver(msg);
        } catch (RuntimeException e) {
            LOGGER.debug("deliver of output {} failed, will retry on a later poll: {}", msg.id(), e.toString());
        } finally {
            delivering.remove(msg.id());
        }
        return true;
    }

    /*
    Client outcomes (Assumption A8)
     */

    // Sends one decided root input's outcome that no other worker is sending.
    private boolean sendOneOutcome() {
        InboxEntry root = decidedRoots.next();
        if (root == null) {
            return false;
        }
        try {
            outcomes.send(root);
        } finally {
            sending.remove(root.id());
        }
        return true;
    }

    /*
    Garbage collection (Section 3.4)
     */

    // Runs one collection round when gcInterval has elapsed, never two rounds at once (Assumption A5).
    private boolean collectIfDue() {
        if (System.nanoTime() < nextCollectionAtNanos || !collecting.compareAndSet(false, true)) {
            return false;
        }
        try {
            gc.collectOnce();
        } finally {
            nextCollectionAtNanos = System.nanoTime() + gcInterval.toNanos();
            collecting.set(false);
        }
        return true;
    }

    // Work items of one kind: an in-memory queue of claimed candidates, refilled by at most one scan of the durable
    // table per pollInterval (or sooner after wakeUp); a candidate is claimed in `claimed` when it is queued and
    // released by the task that handles it.
    private final class Source<T> {
        private final String name;
        private final Supplier<List<T>> scan;
        private final Function<T, Long> key;
        private final Set<Long> claimed;
        private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean scanning = new AtomicBoolean();
        private volatile long nextScanAtNanos;
        private volatile boolean dirty = true;

        private Source(String name, Supplier<List<T>> scan, Function<T, Long> key, Set<Long> claimed) {
            this.name = name;
            this.scan = scan;
            this.key = key;
            this.claimed = claimed;
        }

        // Returns the next claimed candidate, scanning the table first if the queue is empty and a scan is due.
        private T next() {
            T item = queue.poll();
            if (item != null) {
                return item;
            }
            refill();
            return queue.poll();
        }

        // Makes the next next() scan immediately.
        private void markDirty() {
            dirty = true;
        }

        // Scans the table and queues every unclaimed candidate; only one worker scans at a time.
        private void refill() {
            if ((!dirty && System.nanoTime() < nextScanAtNanos) || !scanning.compareAndSet(false, true)) {
                return;
            }
            try {
                dirty = false;
                for (T item : scan.get()) {
                    if (claimed.add(key.apply(item))) {
                        queue.add(item);
                    }
                }
            } catch (RuntimeException e) {
                LOGGER.warn("{} scan failed, will retry", name, e);
            } finally {
                nextScanAtNanos = System.nanoTime() + pollInterval.toNanos();
                scanning.set(false);
            }
        }
    }
}

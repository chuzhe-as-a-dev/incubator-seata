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
package org.apache.seata.rm.datasource.xa;

// The ACTA Inbox of a resource's branches, which the application keeps; a failure is a RuntimeException
public interface ActaInbox {
    // Before a no vote, which needs the durable VOTING
    void recordVoting(long inputId);

    // After the no vote, which may wait for a batch
    void recordVoted(long inputId);

    // Records COMMITTING or ABORTING unless the input has finished, and reads the tid of its latest attempt
    Phase2Start startPhase2(long inputId, int phase2);

    // The end of phase 2, which may wait for a batch
    void recordPhase2(long inputId, int phase2);

    final class Phase2Start {
        private final boolean started;
        // Null without an attempt
        private final String latestTid;

        public Phase2Start(boolean started, String latestTid) {
            this.started = started;
            this.latestTid = latestTid;
        }

        public boolean isStarted() {
            return started;
        }

        public String getLatestTid() {
            return latestTid;
        }
    }
}

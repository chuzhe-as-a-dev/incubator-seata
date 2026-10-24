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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ACTA metadata operations of finish(inId, decision) (ACTA paper Figure 4) that
 * {@link ResourceManagerXA} runs around the XA commit or rollback of an ACTA branch, whose branchId is the input
 * message ID. The ACTA service of each resource registers its hook when it starts; phase 2 of an ACTA branch whose
 * service has not started yet is reported as retryable.
 */
public interface ActaPhase2Hook {

    /**
     * Durably records COMMITTING or ABORTING for the input unless its state is already terminal (Figure 4 lines
     * 76-82).
     *
     * @param inId   the input message ID
     * @param commit true for COMMIT, false for ABORT
     * @return whether an attempt of the input recorded a local transaction, i.e. whether there is an XA branch to
     *         finish in the database (Figure 4 line 83)
     */
    boolean enterPhase2(long inId, boolean commit);

    /**
     * Durably records COMMITTED or ABORTED after the XA branch was finished, if the input is still present
     * (Figure 4 lines 85-88), and lets the service react to the applied decision.
     *
     * @param inId   the input message ID
     * @param commit true for COMMIT, false for ABORT
     */
    void completePhase2(long inId, boolean commit);

    /**
     * Registry of the hooks by Seata resource ID.
     */
    final class Registry {
        private static final Map<String, ActaPhase2Hook> HOOKS = new ConcurrentHashMap<>();

        private Registry() {}

        public static void register(String resourceId, ActaPhase2Hook hook) {
            HOOKS.put(resourceId, hook);
        }

        public static ActaPhase2Hook of(String resourceId) {
            return HOOKS.get(resourceId);
        }
    }
}

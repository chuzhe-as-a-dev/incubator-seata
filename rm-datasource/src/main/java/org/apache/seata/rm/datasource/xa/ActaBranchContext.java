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

/**
 * Activation context of an ACTA participant on the processing thread: while an input message ID is bound,
 * {@link ConnectionProxyXA#setAutoCommit(boolean)} registers the XA branch under that ID (branchId = inId, Figure 3
 * line 18 of the ACTA paper) and marks it with {@link #APPLICATION_DATA}, so that the branch's prepare reports the
 * yes vote and its phase 2 runs the ACTA metadata operations. Ordinary XA branches of the same process never bind
 * a context and are unaffected.
 */
public final class ActaBranchContext {

    /**
     * Application data of an ACTA branch; phase-2 threads recognise ACTA branches by it (they cannot see the
     * processing thread's context), so every branch report of such a branch carries it too, because the TC
     * overwrites the stored application data with the reported one.
     */
    public static final String APPLICATION_DATA = "acta";

    private static final ThreadLocal<Long> IN_ID = new ThreadLocal<>();

    private ActaBranchContext() {}

    /**
     * Binds the input message ID of the activation that the current thread is about to execute.
     *
     * @param inId the input message ID, a positive long
     */
    public static void bind(long inId) {
        if (inId <= 0L) {
            throw new IllegalArgumentException("ACTA input message ID must be positive: " + inId);
        }
        IN_ID.set(inId);
    }

    /**
     * Unbinds the activation context of the current thread.
     */
    public static void unbind() {
        IN_ID.remove();
    }

    /**
     * Returns the bound input message ID, or null when the current thread runs no ACTA activation.
     *
     * @return the input message ID or null
     */
    public static Long current() {
        return IN_ID.get();
    }

    /**
     * Returns whether a branch was registered by an ACTA activation.
     *
     * @param applicationData the branch's application data
     * @return true for an ACTA branch
     */
    public static boolean isActaBranch(String applicationData) {
        return APPLICATION_DATA.equals(applicationData);
    }
}

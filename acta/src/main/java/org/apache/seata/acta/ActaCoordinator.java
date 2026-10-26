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

import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.exception.TransactionExceptionCode;
import org.apache.seata.tm.api.GlobalTransaction;
import org.apache.seata.tm.api.GlobalTransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

// The paper's 2PC coordinator interface (Section 2.1, Assumption A7) on the Seata transaction coordinator. The
// participant-side register and voteYes are issued by Seata's XA connection proxy when an activation starts and
// prepares its branch (ActaBranchContext, ConnectionProxyXA); this class carries the remaining calls: the
// participant's voteNo, and the root side's begin and the commit-request control output. Phase-2 decisions arrive
// through Seata's XA resource manager and ActaService.enterPhase2 / completePhase2.
public class ActaCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActaCoordinator.class);

    // Effectively no coordinator-side timeout: the coordinator never converts a timeout into an abort (A7).
    private static final int GLOBAL_TIMEOUT_MILLIS = Integer.MAX_VALUE;
    private static final long RETRY_DELAY_MILLIS = 200;

    /*
    Participant side (Figure 3 line 52; Figure 7 line 153)
     */

    // 2PC::voteNo(gtid, inId): makes the coordinator decide abort for the workflow; idempotent on Seata.
    public void voteNo(String gtid, long inId) {
        while (true) {
            try {
                GlobalTransactionContext.reload(gtid).rollback();
                return;
            } catch (TransactionException e) {
                retryLater("voteNo " + inId + " in " + gtid, e);
            }
        }
    }

    // Discards a global transaction nothing was admitted under (the root turned out to be a resubmission or
    // retired): rolls it back at the coordinator so it does not linger in the coordinator's store; idempotent.
    public void abandon(String gtid) {
        while (true) {
            try {
                GlobalTransactionContext.reload(gtid).rollback();
                return;
            } catch (TransactionException e) {
                retryLater("abandon " + gtid, e);
            }
        }
    }

    // Returns whether a failed branch start (2PC::register) is a definitive rejection, i.e. the coordinator already
    // decided for the workflow, as opposed to a communication failure.
    public static boolean isRejectedRegistration(SQLException failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof TransactionException te) {
                return isRejected(te.getCode());
            }
        }
        return false;
    }

    /*
    Root side (Section 2.1; Appendix A.1 commit-request control output)
     */

    // Asks the coordinator to create a global transaction and returns its gtid.
    public String begin() {
        while (true) {
            try {
                GlobalTransaction tx = GlobalTransactionContext.createNew();
                tx.begin(GLOBAL_TIMEOUT_MILLIS, "acta");
                RootContext.unbind();
                return tx.getXid();
            } catch (TransactionException e) {
                retryLater("begin", e);
            }
        }
    }

    // Delivers the commit-request control output to the coordinator; idempotent on Seata.
    public void requestCommit(String gtid) {
        while (true) {
            try {
                GlobalTransactionContext.reload(gtid).commit();
                return;
            } catch (TransactionException e) {
                retryLater("requestCommit " + gtid, e);
            }
        }
    }

    // Returns whether a registration failure is a definitive rejection, i.e. the coordinator already decided.
    private static boolean isRejected(TransactionExceptionCode code) {
        return code == TransactionExceptionCode.GlobalTransactionNotExist
                || code == TransactionExceptionCode.GlobalTransactionNotActive
                || code == TransactionExceptionCode.GlobalTransactionStatusInvalid;
    }

    // Waits before retrying a coordinator RPC whose outcome is not definitive (A7: a live caller retries).
    private static void retryLater(String operation, TransactionException e) {
        LOGGER.warn("Coordinator RPC {} failed with {}, retrying", operation, e.getCode());
        try {
            Thread.sleep(RETRY_DELAY_MILLIS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while retrying " + operation, e);
        }
    }
}

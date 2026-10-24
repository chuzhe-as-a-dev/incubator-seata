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

import org.apache.seata.common.DefaultValues;
import org.apache.seata.common.lock.ResourceLock;
import org.apache.seata.common.thread.NamedThreadFactory;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.Resource;
import org.apache.seata.rm.BaseDataSourceResource;
import org.apache.seata.rm.datasource.AbstractDataSourceCacheResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.seata.core.constants.ConfigurationKeys.XA_CONNECTION_TWO_PHASE_HOLD_TIMEOUT;

/**
 * RM for XA mode.
 *
 */
public class ResourceManagerXA extends AbstractDataSourceCacheResourceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManagerXA.class);

    private static final int TWO_PHASE_HOLD_TIMEOUT = ConfigurationFactory.getInstance()
            .getInt(XA_CONNECTION_TWO_PHASE_HOLD_TIMEOUT, DefaultValues.DEFAULT_XA_CONNECTION_TWO_PHASE_HOLD_TIMEOUT);

    private static final long SCHEDULE_DELAY_MILLS = 60 * 1000L;
    private static final long SCHEDULE_INTERVAL_MILLS = 1000L;
    /**
     * The Timer check xa branch two phase hold timeout.
     */
    protected volatile ScheduledExecutorService xaTwoPhaseTimeoutChecker;

    private final ResourceLock resourceLock = new ResourceLock();

    @Override
    public void init() {
        LOGGER.info("ResourceManagerXA init ...");
    }

    public void initXaTwoPhaseTimeoutChecker() {
        if (xaTwoPhaseTimeoutChecker == null) {
            try (ResourceLock ignored = resourceLock.obtain()) {
                if (xaTwoPhaseTimeoutChecker == null) {
                    boolean shouldBeHold = dataSourceCache.values().parallelStream()
                            .anyMatch(resource -> {
                                if (resource instanceof DataSourceProxyXA) {
                                    return ((DataSourceProxyXA) resource).isShouldBeHeld();
                                }
                                return false;
                            });
                    if (shouldBeHold) {
                        xaTwoPhaseTimeoutChecker = new ScheduledThreadPoolExecutor(
                                1, new NamedThreadFactory("xaTwoPhaseTimeoutChecker", 1, true));
                        xaTwoPhaseTimeoutChecker.scheduleAtFixedRate(
                                () -> {
                                    for (Map.Entry<String, Resource> entry : dataSourceCache.entrySet()) {
                                        BaseDataSourceResource resource = (BaseDataSourceResource) entry.getValue();
                                        if (resource.isShouldBeHeld()) {
                                            if (resource instanceof DataSourceProxyXA) {
                                                Map<String, ConnectionProxyXA> keeper = resource.getKeeper();
                                                for (Map.Entry<String, ConnectionProxyXA> connectionEntry :
                                                        keeper.entrySet()) {
                                                    ConnectionProxyXA connection = connectionEntry.getValue();
                                                    long now = System.currentTimeMillis();
                                                    try (ResourceLock ignored2 = connection
                                                            .getResourceLock()
                                                            .obtain()) {
                                                        if (connection.getPrepareTime() != null
                                                                && now - connection.getPrepareTime()
                                                                        > TWO_PHASE_HOLD_TIMEOUT) {
                                                            try {
                                                                connection.closeForce();
                                                            } catch (SQLException e) {
                                                                LOGGER.warn(
                                                                        "Force close the xa physical connection fail",
                                                                        e);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                SCHEDULE_DELAY_MILLS,
                                SCHEDULE_INTERVAL_MILLS,
                                TimeUnit.MILLISECONDS);
                    }
                }
            }
        }
    }

    @Override
    public BranchType getBranchType() {
        return BranchType.XA;
    }

    @Override
    public BranchStatus branchCommit(
            BranchType branchType, String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException {
        return finishBranch(true, branchType, xid, branchId, resourceId, applicationData);
    }

    @Override
    public BranchStatus branchRollback(
            BranchType branchType, String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException {
        return finishBranch(false, branchType, xid, branchId, resourceId, applicationData);
    }

    private BranchStatus finishBranch(
            boolean committed,
            BranchType branchType,
            String xid,
            long branchId,
            String resourceId,
            String applicationData)
            throws TransactionException {
        XAXid xaBranchXid = XAXidBuilder.build(xid, branchId);
        Resource resource = dataSourceCache.get(resourceId);
        // ACTA branch (branchId = input message ID): finish(inId, decision) of the ACTA paper (Figure 4) wraps the
        // XA finish in the durable COMMITTING/ABORTING and COMMITTED/ABORTED metadata transitions.
        ActaPhase2Hook acta = null;
        if (ActaBranchContext.isActaBranch(applicationData)) {
            acta = ActaPhase2Hook.Registry.of(resourceId);
            if (acta == null) {
                LOGGER.warn("No ACTA service for resource {} yet, asking the TC to retry phase 2 of {}", resourceId,
                        xaBranchXid);
                return actaRetryable(committed);
            }
            boolean attempted;
            try {
                attempted = acta.enterPhase2(branchId, committed);
            } catch (RuntimeException e) {
                LOGGER.error("ACTA enterPhase2 of {} failed, asking the TC to retry", xaBranchXid, e);
                return actaRetryable(committed);
            }
            if (!attempted) {
                // No attempt of the input recorded a local transaction, so there is nothing to finish in the
                // database (Figure 4 line 83); an attempt that is still executing sees the recorded decision when
                // it tries to publish, refuses, and rolls its own XA branch back.
                return actaComplete(acta, branchId, committed, xaBranchXid);
            }
        }
        if (resource instanceof AbstractDataSourceProxyXA) {
            try (ConnectionProxyXA connectionProxyXA =
                    ((AbstractDataSourceProxyXA) resource).getConnectionForXAFinish(xaBranchXid)) {
                if (committed) {
                    connectionProxyXA.xaCommit(xid, branchId, applicationData);
                    LOGGER.info(xaBranchXid + " was committed.");
                    return acta == null
                            ? BranchStatus.PhaseTwo_Committed
                            : actaComplete(acta, branchId, true, xaBranchXid);
                } else {
                    connectionProxyXA.xaRollback(xid, branchId, applicationData);
                    LOGGER.info(xaBranchXid + " was rollbacked");
                    return acta == null
                            ? BranchStatus.PhaseTwo_Rollbacked
                            : actaComplete(acta, branchId, false, xaBranchXid);
                }
            } catch (XAException | SQLException sqle) {
                if (acta != null && isAlreadyFinalized(sqle)) {
                    // DB::finishTxn is idempotent (ACTA paper, Figure 4): the branch was already rolled back by its
                    // own attempt (abortAndVote, a refused prepare) or already finished by an earlier phase-2 call
                    // whose acknowledgment was lost, so the decision is applied to the metadata and acknowledged.
                    LOGGER.info("{} was already {} (idempotent ACTA phase 2)", xaBranchXid,
                            committed ? "committed" : "rolled back");
                    return actaComplete(acta, branchId, committed, xaBranchXid);
                }
                if (sqle instanceof XAException) {
                    try {
                        if (((XAException) sqle).errorCode == XAException.XAER_NOTA) {
                            if (committed) {
                                return BranchStatus.PhaseTwo_CommitFailed_XAER_NOTA_Retryable;
                            } else {
                                return BranchStatus.PhaseTwo_RollbackFailed_XAER_NOTA_Retryable;
                            }
                        } else if (((XAException) sqle).errorCode == XAException.XA_RETRY) {
                            if (committed) {
                                return BranchStatus.PhaseTwo_CommitFailed_Retryable;
                            } else {
                                return BranchStatus.PhaseTwo_RollbackFailed_Retryable;
                            }
                        }
                    } finally {
                        BaseDataSourceResource.setBranchStatus(
                                xaBranchXid.toString(),
                                committed ? BranchStatus.PhaseTwo_Committed : BranchStatus.PhaseTwo_Rollbacked);
                    }
                }
                if (committed) {
                    LOGGER.error(xaBranchXid + " commit failed since " + sqle.getMessage(), sqle);
                    // FIXME: case of PhaseTwo_CommitFailed_Unretryable
                    return BranchStatus.PhaseTwo_CommitFailed_Retryable;
                } else {
                    LOGGER.error(xaBranchXid + " rollback failed since " + sqle.getMessage(), sqle);
                    // FIXME: case of PhaseTwo_RollbackFailed_Unretryable
                    return BranchStatus.PhaseTwo_RollbackFailed_Retryable;
                }
            }
        } else {
            LOGGER.error("Unknown Resource for XA resource " + resourceId + " " + resource);
            if (committed) {
                return BranchStatus.PhaseTwo_CommitFailed_Unretryable;
            } else {
                return BranchStatus.PhaseTwo_RollbackFailed_Unretryable;
            }
        }
    }

    /**
     * Records the terminal ACTA state of the input after its XA branch was finished (Figure 4 lines 85-88); the
     * returned status is the phase-2 acknowledgment, or a retryable failure when the metadata write failed.
     */
    private static BranchStatus actaComplete(ActaPhase2Hook acta, long inId, boolean committed, XAXid xaBranchXid) {
        try {
            acta.completePhase2(inId, committed);
        } catch (RuntimeException e) {
            LOGGER.error("ACTA completePhase2 of {} failed, asking the TC to retry", xaBranchXid, e);
            return actaRetryable(committed);
        }
        return committed ? BranchStatus.PhaseTwo_Committed : BranchStatus.PhaseTwo_Rollbacked;
    }

    // Whether a failed XA commit or rollback means the branch no longer exists in the database because it was
    // already committed or rolled back: MySQL answers XAER_NOTA (error 1397), PostgreSQL reports the missing
    // prepared transaction as SQLSTATE 42704 inside the driver's XAException.
    private static boolean isAlreadyFinalized(Exception failure) {
        if (failure instanceof XAException && ((XAException) failure).errorCode == XAException.XAER_NOTA) {
            return true;
        }
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException) {
                SQLException sqlException = (SQLException) cause;
                if ("42704".equals(sqlException.getSQLState()) || sqlException.getErrorCode() == 1397) {
                    return true;
                }
            }
        }
        return false;
    }

    private static BranchStatus actaRetryable(boolean committed) {
        return committed
                ? BranchStatus.PhaseTwo_CommitFailed_Retryable
                : BranchStatus.PhaseTwo_RollbackFailed_Retryable;
    }
}

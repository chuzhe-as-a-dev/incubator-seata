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
import org.apache.seata.common.exception.ShouldNeverHappenException;
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

import javax.sql.DataSource;
import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    private static final int ACTA_PHASE2_COMMITTING = 0;
    private static final int ACTA_PHASE2_ABORTING = 1;
    private static final int ACTA_PHASE2_COMMITTED = 2;
    private static final int ACTA_PHASE2_ABORTED = 3;

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
        Resource resource = dataSourceCache.get(resourceId);
        if (!(resource instanceof AbstractDataSourceProxyXA)) {
            LOGGER.error("Unknown Resource for XA resource " + resourceId + " " + resource);
            if (committed) {
                return BranchStatus.PhaseTwo_CommitFailed_Unretryable;
            } else {
                return BranchStatus.PhaseTwo_RollbackFailed_Unretryable;
            }
        }
        if (DataSourceProxyXA.SHOULD_ENABLE_ACTA) {
            return finishActaBranch(committed, (AbstractDataSourceProxyXA) resource, branchId);
        }
        return finishXaBranch(committed, (AbstractDataSourceProxyXA) resource, XAXidBuilder.build(xid, branchId));
    }

    private BranchStatus finishXaBranch(boolean committed, AbstractDataSourceProxyXA resource, XAXid xaBranchXid) {
        try (ConnectionProxyXA connectionProxyXA = resource.getConnectionForXAFinish(xaBranchXid)) {
            if (committed) {
                connectionProxyXA.xaCommit(xaBranchXid.getGlobalXid(), xaBranchXid.getBranchId(), null);
                LOGGER.info(xaBranchXid + " was committed.");
                return BranchStatus.PhaseTwo_Committed;
            } else {
                connectionProxyXA.xaRollback(xaBranchXid.getGlobalXid(), xaBranchXid.getBranchId(), null);
                LOGGER.info(xaBranchXid + " was rollbacked");
                return BranchStatus.PhaseTwo_Rollbacked;
            }
        } catch (XAException | SQLException sqle) {
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
    }

    // ACTA's phase 2 of input branchId: record it in acta_inbox around finishing the input's latest attempt, its tid
    private BranchStatus finishActaBranch(boolean committed, AbstractDataSourceProxyXA resource, long inputId) {
        BranchStatus finished = committed ? BranchStatus.PhaseTwo_Committed : BranchStatus.PhaseTwo_Rollbacked;
        BranchStatus retry =
                committed ? BranchStatus.PhaseTwo_CommitFailed_Retryable : BranchStatus.PhaseTwo_RollbackFailed_Retryable;
        DataSource metadata = resource.getTargetDataSource();
        XAXid tid;
        try (Connection connection = metadata.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (updateActaPhaseTwo(connection, inputId, committed ? ACTA_PHASE2_COMMITTING : ACTA_PHASE2_ABORTING)
                        == 0) {
                    connection.commit();
                    return finished;
                }
                tid = selectActaTid(connection, inputId);
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to start phase 2 of ACTA input " + inputId, e);
            return retry;
        }

        BranchStatus status;
        if (tid == null) {
            if (committed) {
                throw new ShouldNeverHappenException("ACTA input " + inputId + " committed without a prepared attempt");
            }
            status = finished;
        } else if (!committed && ((DataSourceProxyXA) resource).abortUnpreparedActaAttempt(tid)) {
            status = finished;
        } else {
            status = finishXaBranch(committed, resource, tid);
            // An unknown tid has been finished, since it is no longer an unprepared attempt
            if (status == BranchStatus.PhaseTwo_CommitFailed_XAER_NOTA_Retryable
                    || status == BranchStatus.PhaseTwo_RollbackFailed_XAER_NOTA_Retryable) {
                status = finished;
            }
        }
        if (status == finished) {
            try (Connection connection = metadata.getConnection()) {
                updateActaPhaseTwo(connection, inputId, committed ? ACTA_PHASE2_COMMITTED : ACTA_PHASE2_ABORTED);
            } catch (SQLException e) {
                LOGGER.error("Failed to complete phase 2 of ACTA input " + inputId, e);
                return retry;
            }
        }
        return status;
    }

    private static int updateActaPhaseTwo(Connection connection, long inputId, int phase2) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "update acta_inbox set phase2 = ? where id = ? and (phase2 is null or phase2 not in (?, ?))")) {
            statement.setInt(1, phase2);
            statement.setLong(2, inputId);
            statement.setInt(3, ACTA_PHASE2_COMMITTED);
            statement.setInt(4, ACTA_PHASE2_ABORTED);
            return statement.executeUpdate();
        }
    }

    private static XAXid selectActaTid(Connection connection, long inputId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select tid from acta_inbox where id = ?")) {
            statement.setLong(1, inputId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                String tid = resultSet.getString(1);
                if (tid == null) {
                    return null;
                }
                int separator = tid.lastIndexOf('-');
                return XAXidBuilder.build(tid.substring(0, separator), Long.parseLong(tid.substring(separator + 1)));
            }
        }
    }
}

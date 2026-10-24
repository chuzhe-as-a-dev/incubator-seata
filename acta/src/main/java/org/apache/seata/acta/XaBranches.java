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

import org.apache.seata.acta.MetadataStore.Dialect;
import org.apache.seata.rm.datasource.xa.XAXid;
import org.apache.seata.rm.datasource.xa.XAXidBuilder;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

// The paper's DB::isPrepared(tid) primitive (Assumption A6, Figure 5 line 95) over the XA branches that Seata's
// ConnectionProxyXA starts and prepares. A tid is the text form of the branch's XA identifier, XABranchXid.toString()
// = "<xid>-<branchId>", whose gtrid is the Seata xid and whose bqual is "-<branchId>" (format 9752). MySQL's
// XA RECOVER lists that text as `data`; PostgreSQL's pg_prepared_xacts lists the JDBC driver's gid encoding of the
// same identifier. Queries run on plain connections, independently of the connection that prepared the branch.
public final class XaBranches {

    private final DataSource dataSource;
    private final Dialect dialect;

    public XaBranches(DataSource dataSource, Dialect dialect) {
        this.dataSource = dataSource;
        this.dialect = dialect;
    }

    // DB::isPrepared(tid): whether the branch is currently prepared in the database.
    public boolean isPrepared(String tid) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            if (dialect == Dialect.MYSQL) {
                XAXid xaXid = parse(tid);
                try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("XA RECOVER")) {
                    while (rs.next()) {
                        if (rs.getLong("formatID") == xaXid.getFormatId() && tid.equals(rs.getString("data"))) {
                            return true;
                        }
                    }
                    return false;
                }
            }
            try (var select = connection.prepareStatement("select 1 from pg_prepared_xacts where gid = ?")) {
                select.setString(1, pgGid(parse(tid)));
                try (ResultSet rs = select.executeQuery()) {
                    return rs.next();
                }
            }
        } catch (SQLException e) {
            throw new ActaException("isPrepared " + tid, e);
        }
    }

    // Rolls back every prepared transaction in the database; only for resetting state between benchmark runs,
    // never during recovery, which must retain prepared transactions (A6).
    public int rollbackAllPrepared() {
        try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {
            connection.setAutoCommit(true);
            List<String> commands = new ArrayList<>();
            if (dialect == Dialect.MYSQL) {
                try (ResultSet rs = stmt.executeQuery("XA RECOVER")) {
                    while (rs.next()) {
                        String data = rs.getString("data");
                        int gtridLength = rs.getInt("gtrid_length");
                        commands.add("XA ROLLBACK '" + data.substring(0, gtridLength) + "','"
                                + data.substring(gtridLength) + "'," + rs.getLong("formatID"));
                    }
                }
            } else {
                try (ResultSet rs = stmt.executeQuery("select gid from pg_prepared_xacts")) {
                    while (rs.next()) {
                        commands.add("ROLLBACK PREPARED '" + rs.getString(1) + "'");
                    }
                }
            }
            for (String command : commands) {
                stmt.execute(command);
            }
            return commands.size();
        } catch (SQLException e) {
            throw new ActaException("rollbackAllPrepared", e);
        }
    }

    // Rebuilds the XA identifier from its text form.
    static XAXid parse(String tid) {
        int dash = tid.lastIndexOf('-');
        if (dash <= 0 || dash == tid.length() - 1) {
            throw new IllegalArgumentException("Not an XA branch identifier: " + tid);
        }
        return XAXidBuilder.build(tid.substring(0, dash), Long.parseLong(tid.substring(dash + 1)));
    }

    // The PostgreSQL JDBC driver's gid of an XA identifier (org.postgresql.xa.RecoveredXid.xidToString):
    // formatId_base64(gtrid)_base64(bqual).
    static String pgGid(XAXid xaXid) {
        Base64.Encoder base64 = Base64.getEncoder();
        return xaXid.getFormatId() + "_" + base64.encodeToString(xaXid.getGlobalTransactionId()) + "_"
                + base64.encodeToString(xaXid.getBranchQualifier());
    }
}

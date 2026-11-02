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
package org.apache.seata.server.session;

import org.apache.seata.config.Configuration;
import org.apache.seata.config.ConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;

import static org.apache.seata.common.ConfigurationKeys.ACTA_ENABLED;
import static org.apache.seata.common.ConfigurationKeys.ACTA_REDIS;
import static org.apache.seata.common.DefaultValues.DEFAULT_ACTA_ENABLED;

// Publishes the outcomes of ACTA global transactions through Redis to the benchmark waiting for them
public final class ActaOutcomes {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActaOutcomes.class);
    private static final String CHANNEL = "acta:outcomes";
    private static final String ACTA_TRANSACTION_NAME_PREFIX = "acta-";
    // Null unless ACTA is enabled
    private static JedisPool redis;

    private ActaOutcomes() {}

    public static void init() {
        Configuration config = ConfigurationFactory.getInstance();
        if (config.getBoolean(ACTA_ENABLED, DEFAULT_ACTA_ENABLED)) {
            redis = new JedisPool(URI.create(config.getConfig(ACTA_REDIS)));
        }
    }

    // The message is "<xid>,<outcome>"
    static void publish(GlobalSession session, String outcome) {
        String name = session.getTransactionName();
        if (redis == null || name == null || !name.startsWith(ACTA_TRANSACTION_NAME_PREFIX)) {
            return;
        }
        try (Jedis jedis = redis.getResource()) {
            jedis.publish(CHANNEL, session.getXid() + "," + outcome);
        } catch (RuntimeException e) {
            LOGGER.error("Failed to publish the outcome {} of {}", outcome, session.getXid(), e);
        }
    }
}

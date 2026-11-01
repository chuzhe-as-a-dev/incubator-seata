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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;

// Publishes the outcomes of ACTA global transactions through Redis to the benchmark waiting for them
final class ActaOutcomes {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActaOutcomes.class);
    private static final String CHANNEL = "acta:outcomes";
    private static final String ACTA_TRANSACTION_NAME_PREFIX = "acta-";
    // Unset unless the TC publishes ACTA outcomes, e.g. redis://localhost:6379
    private static final String REDIS_URI = System.getenv("ACTA_OUTCOMES_REDIS");
    private static final JedisPool REDIS = REDIS_URI == null ? null : new JedisPool(URI.create(REDIS_URI));

    private ActaOutcomes() {}

    // The message is "<xid>,<outcome>"
    static void publish(GlobalSession session, String outcome) {
        String name = session.getTransactionName();
        if (REDIS == null || name == null || !name.startsWith(ACTA_TRANSACTION_NAME_PREFIX)) {
            return;
        }
        try (Jedis jedis = REDIS.getResource()) {
            jedis.publish(CHANNEL, session.getXid() + "," + outcome);
        } catch (RuntimeException e) {
            LOGGER.error("Failed to publish the outcome {} of {}", outcome, session.getXid(), e);
        }
    }
}

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

import org.apache.seata.acta.Metadata.IdLayout;
import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.common.DefaultValues;
import org.apache.seata.config.ConfigurationFactory;

// The ACTA settings of this process (seata.acta.* in a Spring Boot application): the switch that enables the
// built-in ACTA logic, the metadata table prefix, and the deployment-wide message ID layout.
public final class ActaConfig {

    private ActaConfig() {
    }

    // Whether the built-in ACTA protocol logic is enabled in this process; a process that runs ordinary XA
    // benchmarks leaves it off and never touches ACTA metadata.
    public static boolean enabled() {
        return ConfigurationFactory.getInstance()
                .getBoolean(ConfigurationKeys.ACTA_ENABLE, DefaultValues.DEFAULT_ACTA_ENABLE);
    }

    // Prefix of the metadata table names (inbox, outbox, retire_watermark, service, peer).
    public static String tablePrefix() {
        return ConfigurationFactory.getInstance()
                .getConfig(ConfigurationKeys.ACTA_TABLE_PREFIX, DefaultValues.DEFAULT_ACTA_TABLE_PREFIX);
    }

    // The packed message ID layout; it must be the same on every host of a deployment.
    public static IdLayout idLayout() {
        var config = ConfigurationFactory.getInstance();
        return new IdLayout(
                config.getInt(ConfigurationKeys.ACTA_ID_SOURCE_BITS, DefaultValues.DEFAULT_ACTA_ID_SOURCE_BITS),
                config.getInt(ConfigurationKeys.ACTA_ID_TARGET_BITS, DefaultValues.DEFAULT_ACTA_ID_TARGET_BITS),
                config.getInt(ConfigurationKeys.ACTA_ID_EPOCH_BITS, DefaultValues.DEFAULT_ACTA_ID_EPOCH_BITS));
    }
}

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
package org.apache.seata.spring.boot.autoconfigure.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import static org.apache.seata.spring.boot.autoconfigure.StarterConstants.ACTA_PREFIX;

/**
 * seata.acta.*: the built-in ACTA protocol switch, the metadata table prefix, and the message ID layout.
 */
@Component
@ConfigurationProperties(prefix = ACTA_PREFIX)
public class ActaProperties {
    private boolean enable;
    private String tablePrefix;
    private int idSourceBits;
    private int idTargetBits;
    private int idEpochBits;

    public boolean isEnable() {
        return enable;
    }

    public ActaProperties setEnable(boolean enable) {
        this.enable = enable;
        return this;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public ActaProperties setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public int getIdSourceBits() {
        return idSourceBits;
    }

    public ActaProperties setIdSourceBits(int idSourceBits) {
        this.idSourceBits = idSourceBits;
        return this;
    }

    public int getIdTargetBits() {
        return idTargetBits;
    }

    public ActaProperties setIdTargetBits(int idTargetBits) {
        this.idTargetBits = idTargetBits;
        return this;
    }

    public int getIdEpochBits() {
        return idEpochBits;
    }

    public ActaProperties setIdEpochBits(int idEpochBits) {
        this.idEpochBits = idEpochBits;
        return this;
    }
}

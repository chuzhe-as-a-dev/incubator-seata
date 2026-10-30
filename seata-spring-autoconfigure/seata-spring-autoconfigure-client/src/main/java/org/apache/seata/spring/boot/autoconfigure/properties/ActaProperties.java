package org.apache.seata.spring.boot.autoconfigure.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import static org.apache.seata.spring.boot.autoconfigure.StarterConstants.ACTA_PREFIX;

@Component
@ConfigurationProperties(prefix = ACTA_PREFIX)
public class ActaProperties {
    private boolean enabled;

    public boolean isEnabled() {
        return enabled;
    }

    public ActaProperties setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }
}

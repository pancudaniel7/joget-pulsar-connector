package org.joget.pulsar;

import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.service.AppUtil;
import org.joget.plugin.base.DefaultAuditTrailPlugin;

import java.util.Map;

public class PulsarConsumerAuditTrail extends DefaultAuditTrailPlugin {

    @Override
    public String getName() {
        return "Pulse Consumer Audit Trail";
    }

    @Override
    public String getVersion() {
        return "7.0.0";
    }

    @Override
    public String getDescription() {
        return "Tool to consume messages from a Pulsar topic";
    }

    @Override
    public String getLabel() {
        return "Pulsar Consumer Audit Trail";
    }

    @Override
    public String getClassName() {
        return getClass().getName();
    }

    @Override
    public String getPropertyOptions() {
        AppDefinition appDef = AppUtil.getCurrentAppDefinition();
        String appId = appDef.getId();
        String appVersion = appDef.getVersion().toString();
        Object[] arguments = new Object[]{appId, appVersion, appId, appVersion};
        return AppUtil.readPluginResource(getClass().getName(), "/properties/pulsarConsumerAuditTrail.json", arguments, true, "messages/pulsarMessages");
    }

    @Override
    public Object execute(Map map) {
        // no action required, background processing is handled by ConsumerThreadMonitor.
        return null;
    }
    
}

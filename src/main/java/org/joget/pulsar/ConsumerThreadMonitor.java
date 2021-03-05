package org.joget.pulsar;

import org.apache.pulsar.client.api.PulsarClientException;
import org.joget.apps.app.dao.PluginDefaultPropertiesDao;
import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.model.PluginDefaultProperties;
import org.joget.apps.app.service.AppPluginUtil;
import org.joget.apps.app.service.AppService;
import org.joget.apps.app.service.AppUtil;
import org.joget.commons.util.LogUtil;
import org.joget.commons.util.PluginThread;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

import java.util.*;

import static java.lang.String.format;

/**
 * Monitoring thread that polls published apps for matching audit trail plugins.
 * New threads will be created for newly configured or modified plugins
 * and unused threads will be stopped.
 */
public class ConsumerThreadMonitor implements Runnable {

    private static final Object BROKER_URL_KEY = "broker_url_key";
    private static final Object TOPIC_NAME = "topic";


    boolean stopThreadMonitor = false;
    int threadMonitorInterval = 10000; // 10s

    static Collection<String> pluginConfigs = new HashSet<>(); // appId_json
    static Map<String, ConsumerRunnable> runnables = new HashMap<>(); // appId -> Thread

    @Override
    public void run() {
        LogUtil.info(getClass().getName(), "Waiting for platform init");
        waitForInit();
        LogUtil.info(getClass().getName(), "Started thread monitor");
        try {
            while (!stopThreadMonitor) {
                monitorRunnables();
                Thread.sleep(threadMonitorInterval);
            }
            cleanup();
        } catch (InterruptedException | PulsarClientException e) {
            LogUtil.error(getClass().getName(), e.getCause(),
                    format("Activator start fail with message: %s", e.getMessage()));
        }
    }

    /**
     * Wait for completion of startup initialization for the platform.
     * This is to prevent premature thread creation on startup.
     */
    protected void waitForInit() {
        try {
            AppUtil appUtil = null;
            ApplicationContext appContext = AppUtil.getApplicationContext();
            if (appContext != null) {
                appUtil = (AppUtil) appContext.getBean("appUtil");
            }
            if (appUtil == null) {
                Thread.sleep(500);
                waitForInit();
            }
        } catch (InterruptedException | BeansException e) {
            LogUtil.error(getClass().getName(), e, "Error retrieving app service " + e.getMessage());
        }
    }

    /**
     * Stop the thread monitor.
     */
    public void shutdown() {
        LogUtil.info(getClass().getName(), "Stopping thread monitor");
        stopThreadMonitor = true;
    }

    /**
     * Stops all running threads.
     */
    public void cleanup() {
        for (String appId : runnables.keySet()) {
            stopRunnable(appId);
        }
        runnables.clear();
        pluginConfigs.clear();
    }

    /**
     * Poll published apps for matching audit trail plugins.
     * New threads will be created for newly configured or modified plugins
     * and unused threads will be stopped.
     */
    public void monitorRunnables() throws PulsarClientException {
        // get published apps
        ApplicationContext appContext = AppUtil.getApplicationContext();
        AppService appService = (AppService) appContext.getBean("appService");
        Collection<AppDefinition> appDefs = appService.getPublishedApps(null);

        // loop thru apps to retrieve configured plugins
        String pluginName = new PulsarConsumerAuditTrail().getName();
        PluginDefaultPropertiesDao dao = (PluginDefaultPropertiesDao) appContext.getBean("pluginDefaultPropertiesDao");
        Map<String, String> pluginMap = new HashMap<>();
        for (AppDefinition appDef : appDefs) {
            Collection<PluginDefaultProperties> pluginDefaultProperties = dao.getPluginDefaultPropertiesList(pluginName, appDef, null, Boolean.TRUE, null, null);
            if (!pluginDefaultProperties.isEmpty()) {
                // get plugin config
                PluginDefaultProperties pluginProperties = pluginDefaultProperties.iterator().next();
                String appId = appDef.getAppId();
                String json = pluginProperties.getPluginProperties();
                String key = appId + "_" + json.hashCode();

                // start threads for new or modified plugins
                if (!pluginConfigs.contains(key)) {
                    if (runnables.containsKey(appId)) {
                        stopRunnable(appId);
                    }
                    ConsumerRunnable newRunnable = startRunnable(appDef, json);
                    if (newRunnable != null) {
                        runnables.put(appId, newRunnable);
                    }
                }
                pluginMap.put(appId, key);
            }
        }
        // stop threads for unconfigured plugins
        for (String appId : runnables.keySet()) {
            String key = pluginMap.get(appId);
            if (key == null) {
                stopRunnable(appId);
                runnables.remove(appId);
            }
        }
        pluginConfigs.clear();
        pluginConfigs.addAll(pluginMap.values());
    }

    /**
     * Start a consumer thread.
     *
     * @param appDef
     * @param json
     * @return The created Thread.
     */
    public ConsumerRunnable startRunnable(AppDefinition appDef, String json) throws PulsarClientException {
        ConsumerRunnable runnable = null;

        // get plugin configuration
        Map propertiesMap = AppPluginUtil.getDefaultProperties(new PulsarConsumerAuditTrail(), json, appDef, null);

        String brokerUrl = (String) propertiesMap.get("brokerUrl");
        String apiKey = (String) propertiesMap.get("apiKey");
        String topic = (String) propertiesMap.get("topic");
        String script = (String) propertiesMap.get("script");

        boolean debug = "true".equalsIgnoreCase((String) propertiesMap.get("debugMode"));

        // set classloader for OSGI
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextClassLoader = currentThread.getContextClassLoader();
        String subscription = generateSubscriptionName(appDef.getName());

        try {
            currentThread.setContextClassLoader(this.getClass().getClassLoader());

            // start thread
            runnable = new ConsumerRunnable(brokerUrl, apiKey, topic, subscription, script, debug);
            PluginThread thread = new PluginThread(runnable);
            thread.start();
        } finally {
            // reset classloader
            currentThread.setContextClassLoader(threadContextClassLoader);
        }

        LogUtil.info(getClass().getName(), "Started Kafka consumer thread for app " + appDef.getAppId());
        if (debug) {
            LogUtil.info(getClass().getName(), "Kafka consumer thread JSON: " + json);
        }
        return runnable;
    }

    private String generateSubscriptionName(String appName) {
        UUID uuid = UUID.randomUUID();
        return String.format("joget-%s-%s", appName, uuid.toString());
    }

    /**
     * Stop a consumer thread.
     *
     * @param appId
     */
    public void stopRunnable(String appId) {
        ConsumerRunnable runnable = runnables.get(appId);
        if (runnable != null) {
            runnable.shutdown();
            LogUtil.info(getClass().getName(), "Stopped Kafka consumer thread for " + appId);
        }
    }
}

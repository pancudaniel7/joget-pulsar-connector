package org.joget.pulsar;

import bsh.Interpreter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.joget.commons.util.LogUtil;

/**
 * Adapted from the samples at
 * https://github.com/ibm-messaging/event-streams-samples Kafka consumer
 * runnable which can be used to create and run consumer as a separate thread.
 */
public class ConsumerRunnable implements Runnable {

    private final Consumer<byte[]> consumer;
    private boolean closing = false;
    private final String script;
    private final boolean debug;

    public ConsumerRunnable(String brokerUrl, String apikey, String topic, String subscription, String script, boolean debugMode) throws PulsarClientException {
        this.script = script;
        this.debug = debugMode;

        PulsarClient client = PulsarClient.builder().serviceUrl(brokerUrl).build();
        this.consumer = client.newConsumer().topic(topic).subscriptionName(subscription).subscribe();
    }

    @Override
    public void run() {
        LogUtil.info(getClass().getName(), "Consumer is starting.");

        while (!closing) {
            Message<byte[]> msg = readMessage(consumer);
            try {
                String key = msg.getKey();
                String value = new String(msg.getValue());

                if (debug) {
                    LogUtil.info(getClass().getName(), "Key=" + key + ". Value=" + value);
                }
                executeScript(script, key, value);
                consumer.acknowledge(msg);
                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                LogUtil.error(getClass().getName(), e, "Producer/Consumer loop has been unexpectedly interrupted");
                shutdown();
            } catch (final Exception e) {
                LogUtil.error(getClass().getName(), e, "Consumer has failed with exception: " + e);
                shutdown();
                consumer.negativeAcknowledge(msg);
            }
        }

        try {
            LogUtil.info(getClass().getName(), "Consumer is shutting down.");
            this.consumer.close();
        } catch (PulsarClientException e) {
            LogUtil.error(getClass().getName(), e, e.getMessage());
        }
    }

    private Message<byte[]> readMessage(Consumer<byte[]> consumer) {
        Message<byte[]> msg = null;
        try {
            msg = this.consumer.receive();
        } catch (PulsarClientException e) {
            LogUtil.error(getClass().getName(), e.getCause(), e.getMessage());
        }
        return msg;
    }

    public void shutdown() {
        closing = true;
    }

    public void executeScript(String script, String key, String value) {
        try {
            Interpreter interpreter = new Interpreter();
            interpreter.setClassLoader(getClass().getClassLoader());
            interpreter.set("key", key);
            interpreter.set("value", value);

            Object result = interpreter.eval(script);
            LogUtil.info(getClass().getName(), "Script execution complete.");
            if (debug) {
                LogUtil.info(getClass().getName(), "Script result: " + result);
            }
        } catch (Exception e) {
            LogUtil.error(getClass().getName(), e, "Error executing script");
        }
    }
}

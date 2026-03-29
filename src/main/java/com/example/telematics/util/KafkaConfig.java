package com.example.telematics.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class KafkaConfig {
    private static final String CONFIG_FILE = "kafka.properties";
    private static final String BOOTSTRAP_SERVERS_KEY = "kafka.bootstrap.servers";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Properties PROPERTIES = loadProperties();

    private KafkaConfig() {
    }

    public static String getBootstrapServers() {
        // Fallback keeps local learning setup runnable even if the config entry is removed by mistake.
        return PROPERTIES.getProperty(BOOTSTRAP_SERVERS_KEY, DEFAULT_BOOTSTRAP_SERVERS);
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();

        try (InputStream inputStream = KafkaConfig.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (inputStream != null) {
                properties.load(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Kafka config from " + CONFIG_FILE, e);
        }

        return properties;
    }
}

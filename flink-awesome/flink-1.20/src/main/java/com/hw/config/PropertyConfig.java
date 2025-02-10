package com.hw.config;

import java.io.InputStream;
import java.util.Properties;

public class PropertyConfig {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = PropertyConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }

    public static int getInt(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }
} 
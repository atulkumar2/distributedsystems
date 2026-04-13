package com.example.telematics.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Singleton Jackson ObjectMapper shared across all modules.
 *
 * Always use JsonUtil.MAPPER — never create a new ObjectMapper instance inline.
 * ObjectMapper is thread-safe once configured; sharing avoids repeated initialisation costs.
 */
public final class JsonUtil {

    /** Reusable, thread-safe ObjectMapper. */
    public static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        // Write timestamps as readable ISO-8601 strings, not epoch numbers
        MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Allow consumers to keep working when newer producers add fields.
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    // Utility class — no instances allowed
    private JsonUtil() {}

    /** Serialise any object to a JSON string. Throws RuntimeException on failure. */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("JSON serialisation failed for " + obj.getClass().getSimpleName(), e);
        }
    }

    /** Deserialise a JSON string into the requested type. Throws RuntimeException on failure. */
    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException("JSON deserialisation failed for type " + type.getSimpleName(), e);
        }
    }
}

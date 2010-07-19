package org.lilycms.util.repo;

import org.codehaus.jackson.JsonNode;

public class JsonUtil {
    public static JsonNode getNode(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        return node.get(prop);
    }

    public static String getString(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isTextual()) {
            throw new JsonFormatException("Not a string property: " + prop);
        }
        return node.get(prop).getTextValue();
    }

    public static String getString(JsonNode node, String prop, String defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isTextual()) {
            throw new JsonFormatException("Not a string property: " + prop);
        }
        return node.get(prop).getTextValue();
    }

    public static boolean getBoolean(JsonNode node, String prop, boolean defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isBoolean()) {
            throw new JsonFormatException("Not a string property: " + prop);
        }
        return node.get(prop).getBooleanValue();
    }

    public static boolean getBoolean(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isBoolean()) {
            throw new JsonFormatException("Not a string property: " + prop);
        }
        return node.get(prop).getBooleanValue();
    }

    public static int getInt(JsonNode node, String prop, int defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isInt()) {
            throw new JsonFormatException("Not an integer property: " + prop);
        }
        return node.get(prop).getIntValue();
    }

    public static int getInt(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isInt()) {
            throw new JsonFormatException("Not an integer property: " + prop);
        }
        return node.get(prop).getIntValue();
    }
}

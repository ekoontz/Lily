package org.lilyproject.rest.providers.json;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.tools.import_.json.*;

import java.util.HashMap;
import java.util.Map;

public class EntityRegistry {
    protected static Map<Class, RegistryEntry> SUPPORTED_TYPES;
    static {
        SUPPORTED_TYPES = new HashMap<Class, RegistryEntry>();
        SUPPORTED_TYPES.put(RecordType.class, new RegistryEntry(RecordTypeReader.INSTANCE, RecordTypeWriter.INSTANCE, "recordType"));
        SUPPORTED_TYPES.put(FieldType.class, new RegistryEntry(FieldTypeReader.INSTANCE, FieldTypeWriter.INSTANCE, "fieldType"));
        SUPPORTED_TYPES.put(Record.class, new RegistryEntry(RecordReader.INSTANCE, RecordWriter.INSTANCE, "record"));
    }

    public static EntityReader findReader(Class clazz) {
        for (Map.Entry<Class, EntityRegistry.RegistryEntry> entry : EntityRegistry.SUPPORTED_TYPES.entrySet()) {
            if (clazz.isAssignableFrom(entry.getKey())) {
                return entry.getValue().getReader();
            }
        }
        throw new RuntimeException("No entity reader for class " + clazz.getName());
    }

    public static RegistryEntry findReaderRegistryEntry(Class clazz) {
        for (Map.Entry<Class, EntityRegistry.RegistryEntry> entry : EntityRegistry.SUPPORTED_TYPES.entrySet()) {
            if (clazz.isAssignableFrom(entry.getKey())) {
                return entry.getValue();
            }
        }
        throw new RuntimeException("No entity reader for class " + clazz.getName());
    }

    public static EntityWriter findWriter(Class clazz) {
        for (Map.Entry<Class, EntityRegistry.RegistryEntry> entry : EntityRegistry.SUPPORTED_TYPES.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return entry.getValue().getWriter();
            }
        }
        throw new RuntimeException("No entity writer for class " + clazz.getName());
    }

    public static class RegistryEntry {
        EntityReader reader;
        EntityWriter writer;
        String propertyName;

        public RegistryEntry(EntityReader reader, EntityWriter writer, String propertyName) {
            this.reader = reader;
            this.writer = writer;
            this.propertyName = propertyName;
        }

        public EntityReader getReader() {
            return reader;
        }

        public EntityWriter getWriter() {
            return writer;
        }

        public String getPropertyName() {
            return propertyName;
        }
    }
}

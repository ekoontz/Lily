package org.lilycms.rest.providers.json;

import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordType;
import org.lilycms.rest.RepositoryEnabled;
import org.lilycms.tools.import_.json.EntityWriter;
import org.lilycms.tools.import_.json.FieldTypeWriter;
import org.lilycms.tools.import_.json.RecordTypeWriter;
import org.lilycms.tools.import_.json.RecordWriter;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseEntityWriter extends RepositoryEnabled {
    protected static Map<Class, EntityWriter> SUPPORTED_TYPES;
    static {
        SUPPORTED_TYPES = new HashMap<Class, EntityWriter>();
        SUPPORTED_TYPES.put(RecordType.class, RecordTypeWriter.INSTANCE);
        SUPPORTED_TYPES.put(FieldType.class, FieldTypeWriter.INSTANCE);
        SUPPORTED_TYPES.put(Record.class, RecordWriter.INSTANCE);
    }

    public boolean isTypeParamSupported(Type genericType) {
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType)genericType;
            Type[] types = pt.getActualTypeArguments();
            if (types.length == 1 && SUPPORTED_TYPES.containsKey(types[0])) {
                return true;
            }
        }

        return false;
    }

    protected EntityWriter getEntityWriter(Type genericType) {
        Class kind = (Class)((ParameterizedType)genericType).getActualTypeArguments()[0];
        return SUPPORTED_TYPES.get(kind);
    }
}

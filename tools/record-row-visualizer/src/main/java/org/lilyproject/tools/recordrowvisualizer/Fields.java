package org.lilyproject.tools.recordrowvisualizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.EncodingUtil;
import org.lilyproject.repository.impl.HBaseTypeManager;

import java.util.*;

public class Fields {
    Map<Long, Map<String, Object>> values = new HashMap<Long, Map<String, Object>>();
    List<Type<FieldType>> fields = new ArrayList<Type<FieldType>>();
    long minVersion = Integer.MAX_VALUE;
    long maxVersion = Integer.MIN_VALUE;

    Object DELETED = new Object();

    public Fields(NavigableMap<byte[], NavigableMap<Long,byte[]>> cf, TypeManager typeMgr, Scope scope) throws Exception {
        // The fields are rotated so that the version is the primary point of access.
        // Also, field values are decoded, etc. 
        for (Map.Entry<byte[], NavigableMap<Long,byte[]>> entry : cf.entrySet()) {
            byte[] column = entry.getKey();
            UUID uuid = new UUID(Bytes.toLong(column, 0, 8), Bytes.toLong(column, 8, 8));
            String fieldId = uuid.toString();

            for (Map.Entry<Long, byte[]> columnEntry : entry.getValue().entrySet()) {
                long version = columnEntry.getKey();
                byte[] value = columnEntry.getValue();

                FieldType fieldType = registerFieldType(fieldId, typeMgr, scope);

                if (fieldType != null) {
                    Map<String, Object> columns = values.get(version);
                    if (columns == null) {
                        columns = new HashMap<String, Object>();
                        values.put(version, columns);
                    }

                    Object decodedValue;
                    if (EncodingUtil.isDeletedField(value)) {
                        decodedValue = DELETED;
                    } else {
                        decodedValue = fieldType.getValueType().fromBytes(EncodingUtil.stripPrefix(value));
                    }
    
                    columns.put(fieldId, decodedValue);
    
                    if (version > maxVersion) {
                        maxVersion = version;
                    }
    
                    if (version < minVersion) {
                        minVersion = version;
                    }
                }
            }
        }
    }

    private FieldType registerFieldType(String fieldId, TypeManager typeMgr, Scope scope) throws Exception {
        for (Type<FieldType> entry : fields) {
            if (entry.id.equals(fieldId))
                return entry.object;
        }

        Type<FieldType> type = new Type<FieldType>();
        type.id = fieldId;
        type.object = typeMgr.getFieldTypeById(fieldId);
        
        // Filter the scope
        if (scope.equals(type.object.getScope())) {
            fields.add(type);
            return type.object;
        }
        return null;
    }

    public List<Type<FieldType>> getFieldTypes() {
        return fields;
    }

    public Object getValue(long version, String fieldId) {
        Map<String, Object> valuesByColumn = values.get(version);
        if (valuesByColumn == null)
            return null;
        return valuesByColumn.get(fieldId);
    }

    public boolean isNull(long version, String fieldId) {
        Map<String, Object> valuesByColumn = values.get(version);
        return valuesByColumn == null || !valuesByColumn.containsKey(fieldId);
    }

    public boolean isDeleted(long version, String fieldId) {
        Map<String, Object> valuesByColumn = values.get(version);
        return valuesByColumn != null && valuesByColumn.get(fieldId) == DELETED;
    }

    public long getMinVersion() {
        return minVersion;
    }

    public long getMaxVersion() {
        return maxVersion;
    }
}

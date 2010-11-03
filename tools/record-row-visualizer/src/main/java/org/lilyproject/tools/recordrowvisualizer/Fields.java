package org.lilyproject.tools.recordrowvisualizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.EncodingUtil;

import java.util.*;

public class Fields {
    Map<Long, Map<String, Object>> values = new HashMap<Long, Map<String, Object>>();
    List<Type<FieldType>> fields = new ArrayList<Type<FieldType>>();
    long minVersion = Integer.MAX_VALUE;
    long maxVersion = Integer.MIN_VALUE;

    Object DELETED = new Object();

    public Fields(NavigableMap<byte[], NavigableMap<Long,byte[]>> cf, TypeManager typeMgr) throws Exception {
        // The fields are rotated so that the version is the primary point of access.
        // Also, field values are decoded, etc. 
        for (Map.Entry<byte[], NavigableMap<Long,byte[]>> entry : cf.entrySet()) {
            byte[] column = entry.getKey();
            String fieldId = Bytes.toString(column);

            for (Map.Entry<Long, byte[]> columnEntry : entry.getValue().entrySet()) {
                long version = columnEntry.getKey();
                byte[] value = columnEntry.getValue();

                Map<String, Object> columns = values.get(version);
                if (columns == null) {
                    columns = new HashMap<String, Object>();
                    values.put(version, columns);
                }

                FieldType fieldType = registerFieldType(fieldId, typeMgr);

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

    private FieldType registerFieldType(String fieldId, TypeManager typeMgr) throws Exception {
        for (Type<FieldType> entry : fields) {
            if (entry.id.equals(fieldId))
                return entry.object;
        }

        Type<FieldType> type = new Type<FieldType>();
        type.id = fieldId;
        type.object = typeMgr.getFieldTypeById(fieldId);
        fields.add(type);

        return type.object;
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

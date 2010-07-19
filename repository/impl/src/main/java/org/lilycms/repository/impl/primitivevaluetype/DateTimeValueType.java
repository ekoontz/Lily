package org.lilycms.repository.impl.primitivevaluetype;

import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.lilycms.repository.api.PrimitiveValueType;

public class DateTimeValueType implements PrimitiveValueType {

    private final String NAME = "DATETIME";

    public String getName() {
        return NAME;
    }

    public DateTime fromBytes(byte[] bytes) {
        return new DateTime(Bytes.toLong(bytes));
    }

    public byte[] toBytes(Object value) {
        // Currently we only store the millis, not the chronology.
        return Bytes.toBytes(((DateTime)value).getMillis());
    }

    public Class getType() {
        return DateTime.class;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + NAME.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        return true;
    }
}

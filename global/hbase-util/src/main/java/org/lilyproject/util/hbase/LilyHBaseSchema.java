package org.lilyproject.util.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class LilyHBaseSchema {
    public static final byte EXISTS_FLAG = (byte) 0;
    public static final byte DELETE_FLAG = (byte) 1;
    public static final byte[] DELETE_MARKER = new byte[] { DELETE_FLAG };

    public static enum Table {
        RECORD("record"),
        TYPE("type");

        public final byte[] bytes;
        public final String name;

        Table(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Column families in the record table.
     */
    public static enum RecordCf {
        DATA("data"),
        SYSTEM("system"),
        WAL_PAYLOAD("wal-payload"),
        WAL_STATE("wal-state"),
        MQ_PAYLOAD("mq-payload"),
        MQ_STATE("mq-state");

        public final byte[] bytes;
        public final String name;

        RecordCf(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Columns in the record table.
     */
    public static enum RecordColumn {
        VERSION("version"),
        LOCK("lock"),
        DELETED("deleted"),
        NON_VERSIONED_RT_ID("nv-rt"),
        NON_VERSIONED_RT_VERSION("nv-rtv"),
        VERSIONED_RT_ID("v-rt"),
        VERSIONED_RT_VERSION("v-rtv"),
        VERSIONED_MUTABLE_RT_ID("vm-rt"),
        VERSIONED_MUTABLE_RT_VERSION("vm-rtv");

        public final byte[] bytes;
        public final String name;

        RecordColumn(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Column families in the type table.
     */
    public static enum TypeCf {
        DATA("data"),
        FIELDTYPE_ENTRY("fieldtype-entry"),
        MIXIN("mixin");

        public final byte[] bytes;
        public final String name;

        TypeCf(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }

    /**
     * Columns in the type table.
     */
    public static enum TypeColumn {
        VERSION("version"),
        RECORDTYPE_NAME("rt"),
        FIELDTYPE_NAME("ft"),
        FIELDTYPE_VALUETYPE("vt"),
        FIELDTYPE_SCOPE("scope"),
        CONCURRENT_COUNTER("cc");

        public final byte[] bytes;
        public final String name;

        TypeColumn(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }
}

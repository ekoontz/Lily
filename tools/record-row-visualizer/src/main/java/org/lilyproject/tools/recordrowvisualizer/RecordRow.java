package org.lilyproject.tools.recordrowvisualizer;

import org.lilyproject.repository.api.RecordId;

import java.util.*;

/**
 * The root data object passed to the template, containing information
 * about the HBase-storage of a Lily record.
 */
public class RecordRow {
    public RecordId recordId;

    // non-versioned system fields
    public VersionedValue<String> lock = new VersionedValue<String>();
    public VersionedValue<Boolean> deleted = new VersionedValue<Boolean>();
    public VersionedValue<Type> nvRecordType = new VersionedValue<Type>();
    public VersionedValue<Type> version = new VersionedValue<Type>();
    public List<String> unknownNvColumns = new ArrayList<String>();

    // versioned system fields
    public VersionedValue<Type> vRecordType = new VersionedValue<Type>();
    public List<String> unknownVColumns = new ArrayList<String>();

    // non-versioned fields
    public Fields nvFields;

    // versioned fields
    public Fields vFields;

    // Row log
    public SortedMap<RowLogKey, List<String>> mqPayload = new TreeMap<RowLogKey, List<String>>();
    public SortedMap<RowLogKey, List<ExecutionData>> mqState = new TreeMap<RowLogKey, List<ExecutionData>>();

    public SortedMap<RowLogKey, List<String>> walPayload = new TreeMap<RowLogKey, List<String>>();
    public SortedMap<RowLogKey, List<ExecutionData>> walState = new TreeMap<RowLogKey, List<ExecutionData>>();

    public List<String> unknownColumnFamilies = new ArrayList<String>();

    public RecordId getRecordId() {
        return recordId;
    }

    public VersionedValue getDeleted() {
        return deleted;
    }

    public VersionedValue<String> getLock() {
        return lock;
    }

    public VersionedValue<Type> getNvRecordType() {
        return nvRecordType;
    }

    public VersionedValue<Type> getVersion() {
        return version;
    }

    public List<String> getUnknownNvColumns() {
        return unknownNvColumns;
    }

    public Fields getNvFields() {
        return nvFields;
    }

    public Fields getvFields() {
        return vFields;
    }

    public VersionedValue<Type> getvRecordType() {
        return vRecordType;
    }

    public List<String> getUnknownVColumns() {
        return unknownVColumns;
    }

    public SortedMap<RowLogKey, List<String>> getMqPayload() {
        return mqPayload;
    }

    public SortedMap<RowLogKey, List<ExecutionData>> getMqState() {
        return mqState;
    }

    public SortedMap<RowLogKey, List<String>> getWalPayload() {
        return walPayload;
    }

    public SortedMap<RowLogKey, List<ExecutionData>> getWalState() {
        return walState;
    }

    public List<String> getUnknownColumnFamilies() {
        return unknownColumnFamilies;
    }
}

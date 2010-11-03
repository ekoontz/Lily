package org.lilyproject.tools.recordrowvisualizer;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A value which can existing in multiple HBase-versions.
 * Versions are in order for easy display in the template.
 */
public class VersionedValue<T> {
    private SortedMap<Long, T> values = new TreeMap<Long, T>();

    public void put(long version, T value) {
        values.put(version, value);
    }

    public SortedMap<Long, T> getValues() {
        return values;
    }

    public T get(long version) {
        return values.get(version);
    }
}

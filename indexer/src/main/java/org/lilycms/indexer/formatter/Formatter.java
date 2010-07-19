package org.lilycms.indexer.formatter;

import org.lilycms.repository.api.ValueType;

import java.util.List;
import java.util.Set;

/**
 * Formats field values to string for transfer to SOLR.
 *
 * <p>Concerning the supports* methods: you should have at least one of singleValue
 * or multiValue return true, and at least one of nonHierarchical or hierarchical return
 * true. Thus if your formatter can only format simple plain values, you should have
 * both supportsSingleValue() and supportsNonHierarchicalValue() return true.
 */
public interface Formatter {
    List<String> format(Object value, ValueType valueType);

    /**
     * Returning an empty set means this formatter accepts any kind of value (even non-built-in
     * value types one might not know about when implementing the formatter).
     */
    Set<String> getSupportedPrimitiveValueTypes();

    /**
     * Returns true if this formatter can format single (= non-multi-value) values.
     */
    boolean supportsSingleValue();

    /**
     * Returns true if this formatter can format multi-valued values.
     */
    boolean supportsMultiValue();

    /**
     * Returns true if this formatter can format non-hierarchical values.
     */
    boolean supportsNonHierarchicalValue();

    /**
     * Returns true if this formatter can format hierarchical values.
     */
    boolean supportsHierarchicalValue();
}

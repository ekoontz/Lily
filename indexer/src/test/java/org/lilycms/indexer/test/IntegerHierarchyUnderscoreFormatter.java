package org.lilycms.indexer.test;

import org.lilycms.indexer.formatter.Formatter;
import org.lilycms.repository.api.HierarchyPath;
import org.lilycms.repository.api.ValueType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class IntegerHierarchyUnderscoreFormatter implements Formatter {
    private static final Set<String> types = Collections.singleton("INTEGER");

    public List<String> format(Object value, ValueType valueType) {
        HierarchyPath path = (HierarchyPath)value;

        StringBuilder builder = new StringBuilder();
        for (Object item : path.getElements()) {
            if (builder.length() > 0)
                builder.append("_");
            builder.append(item);
        }

        return Collections.singletonList(builder.toString());
    }

    public Set<String> getSupportedPrimitiveValueTypes() {
        return types;
    }

    public boolean supportsSingleValue() {
        return true;
    }

    public boolean supportsMultiValue() {
        return false;
    }

    public boolean supportsNonHierarchicalValue() {
        return false;
    }

    public boolean supportsHierarchicalValue() {
        return true;
    }
}

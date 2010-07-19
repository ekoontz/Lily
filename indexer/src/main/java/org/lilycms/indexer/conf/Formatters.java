package org.lilycms.indexer.conf;

import org.lilycms.indexer.formatter.DefaultFormatter;
import org.lilycms.indexer.formatter.Formatter;
import org.lilycms.repository.api.ValueType;

import java.util.*;

public class
        Formatters {
    private List<FormatterEntry> formatters = new ArrayList<FormatterEntry>();
    private Map<String, FormatterEntry> formattersByName = new HashMap<String, FormatterEntry>();

    private Formatter DEFAULT_FORMATTER = new DefaultFormatter();
    
    public Formatter getFormatter(ValueType valueType) {
        for (FormatterEntry entry : formatters) {
            if (!entry.useForTypes.contains(valueType.getPrimitive().getName())
                    && !entry.useForTypes.isEmpty())
                continue;

            if (valueType.isMultiValue() && !entry.multiValue)
                continue;

            if (!valueType.isMultiValue() && !entry.singleValue)
                continue;

            if (valueType.isHierarchical() && !entry.hierarchical)
                continue;

            if (!valueType.isHierarchical() && !entry.nonHierarchical)
                continue;

            return entry.formatter;
        }

        return DEFAULT_FORMATTER;
    }

    protected boolean hasFormatter(String name) {
        return formattersByName.containsKey(name);
    }

    public Formatter getFormatter(String name) {
        // During index configuration parsing, we will validate that all named formatters exist,
        // so the following check should never be true.
        if (!formattersByName.containsKey(name)) {
            throw new RuntimeException("Formatter with the following name does not exist: " + name);
        }

        return formattersByName.get(name).formatter;
    }

    protected void addFormatter(Formatter formatter, String name, Set<String> useForTypes, boolean singleValue,
            boolean multiValue, boolean nonHierarchical, boolean hierarchical) {

        FormatterEntry entry = new FormatterEntry(formatter, name, useForTypes, singleValue, multiValue,
                nonHierarchical, hierarchical);

        formatters.add(entry);

        if (name != null)
            formattersByName.put(name, entry);
    }

    private static class FormatterEntry {
        String name;
        Set<String> useForTypes;
        Formatter formatter;
        boolean singleValue;
        boolean multiValue;
        boolean nonHierarchical;
        boolean hierarchical;

        public FormatterEntry(Formatter formatter, String name, Set<String> useForTypes, boolean singleValue,
            boolean multiValue, boolean nonHierarchical, boolean hierarchical) {
            this.name = name;
            this.useForTypes = useForTypes;
            this.formatter = formatter;
            this.singleValue = singleValue;
            this.multiValue = multiValue;
            this.nonHierarchical = nonHierarchical;
            this.hierarchical = hierarchical;
        }
    }
}

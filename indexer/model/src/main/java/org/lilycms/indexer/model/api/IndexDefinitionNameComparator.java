package org.lilycms.indexer.model.api;

import java.util.Comparator;

public class IndexDefinitionNameComparator implements Comparator<IndexDefinition> {
    public final static IndexDefinitionNameComparator INSTANCE = new IndexDefinitionNameComparator();

    public int compare(IndexDefinition o1, IndexDefinition o2) {
        return o1.getName().compareTo(o2.getName());
    }
}

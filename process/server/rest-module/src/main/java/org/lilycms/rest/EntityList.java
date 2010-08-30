package org.lilycms.rest;

import java.util.Collection;

public class EntityList<T> {
    private Collection<T> records;

    public EntityList(Collection<T> records) {
        this.records = records;
    }

    public Collection<T> getItems() {
        return records;
    }
}

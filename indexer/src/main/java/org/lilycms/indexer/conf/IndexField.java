package org.lilycms.indexer.conf;

public class IndexField {
    private String name;
    private Value value;

    public IndexField(String name, Value value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Value getValue() {
        return value;
    }
}

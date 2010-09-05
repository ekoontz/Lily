package org.lilycms.indexer.model.api;

public class IndexNotFoundException  extends Exception {

    public IndexNotFoundException(String name) {
        super("Index does not exist: " + name);
    }

}
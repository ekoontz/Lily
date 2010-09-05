package org.lilycms.indexer.model.api;

public class IndexExistsException extends Exception {

    public IndexExistsException(String name) {
        super("Index already exists: " + name);
    }

}

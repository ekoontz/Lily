package org.lilycms.repository.api;

public class BlobNotFoundException extends RepositoryException {
    private final Blob blob;

    public BlobNotFoundException(Blob blob) {
        this.blob = blob;
    }

    @Override
    public String getMessage() {
        return "Blob <" + blob + "> could not be found.";
    }
}

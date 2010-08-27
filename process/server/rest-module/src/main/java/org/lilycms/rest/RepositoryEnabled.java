package org.lilycms.rest;

import org.lilycms.repository.api.Repository;
import org.springframework.beans.factory.annotation.Autowired;

public class RepositoryEnabled {
    protected Repository repository;

    @Autowired
    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}

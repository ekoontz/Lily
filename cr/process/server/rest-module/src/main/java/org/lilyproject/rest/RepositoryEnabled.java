package org.lilyproject.rest;

import org.lilyproject.repository.api.Repository;
import org.springframework.beans.factory.annotation.Autowired;

public class RepositoryEnabled {
    protected Repository repository;

    @Autowired
    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}

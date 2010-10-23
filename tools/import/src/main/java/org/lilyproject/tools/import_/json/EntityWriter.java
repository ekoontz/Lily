package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;

public interface EntityWriter<T> {
    ObjectNode toJson(T entity, Repository repository) throws RepositoryException;

    /**
     * Writes the entity to JSON, but does not include a namespace section into it, rather
     * re-uses the given Namespaces object (the namespaces are assumed to be added to a parent
     * object).
     */
    ObjectNode toJson(T entity, Namespaces namespaces, Repository repository) throws RepositoryException;
}

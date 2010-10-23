package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;

public interface EntityReader<T> {
    T fromJson(ObjectNode node, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException;

    T fromJson(ObjectNode node, Repository repository)
            throws JsonFormatException, RepositoryException;
}

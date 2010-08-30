package org.lilycms.tools.import_.json;

import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;

public interface EntityReader<T> {
    T fromJson(ObjectNode node, Namespaces namespaces, Repository repository)
            throws JsonFormatException, RepositoryException;

    T fromJson(ObjectNode node, Repository repository)
            throws JsonFormatException, RepositoryException;
}

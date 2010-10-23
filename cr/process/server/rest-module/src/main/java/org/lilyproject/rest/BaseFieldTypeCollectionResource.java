package org.lilyproject.rest;

import org.lilyproject.repository.api.*;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public abstract class BaseFieldTypeCollectionResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public EntityList<FieldType> get() {
        try {
            return new EntityList<FieldType>(repository.getTypeManager().getFieldTypes());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading field type list.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    protected FieldType processPost(PostAction<FieldType> postAction) {
        if (!postAction.getAction().equals("create")) {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }

        TypeManager typeManager = repository.getTypeManager();

        FieldType fieldType = postAction.getEntity();
        try {
            fieldType = typeManager.createFieldType(fieldType);
        } catch (FieldTypeExistsException e) {
            throw new ResourceException(e, CONFLICT.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error creating field type.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return fieldType;
    }
}

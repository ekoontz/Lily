/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.rest;

import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeExistsException;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public abstract class BaseRecordTypeCollectionResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public EntityList<RecordType> get() {
        try {
            return new EntityList<RecordType>(repository.getTypeManager().getRecordTypes());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record type list.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    protected RecordType processPost(PostAction<RecordType> postAction) {
        if (!postAction.getAction().equals("create")) {
            throw new ResourceException("Unsupported POST action: " + postAction.getAction(), BAD_REQUEST.getStatusCode());
        }

        TypeManager typeManager = repository.getTypeManager();

        RecordType recordType = postAction.getEntity();
        try {
            recordType = typeManager.createRecordType(recordType);
        } catch (RecordTypeExistsException e) {
            throw new ResourceException(e, CONFLICT.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating record type.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

        return recordType;
    }
}

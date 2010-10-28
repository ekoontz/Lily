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

import org.lilyproject.repository.api.*;
import org.lilyproject.tools.import_.core.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.*;

@Path("schema/recordTypeById/{id}")
public class RecordTypeByIdResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public RecordType get(@PathParam("id") String id) {
        try {
            return repository.getTypeManager().getRecordTypeById(id, null);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (TypeException e) {
            throw new ResourceException("Error loading record type with id " + id, e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @PUT
    @Produces("application/json")
    @Consumes("application/json")
    public Response put(@PathParam("id") String id, RecordType recordType) {
        if (recordType.getId() != null && !recordType.getId().equals(id)) {
            throw new ResourceException("Record type id in submitted field type does not match the id in URI.",
                    BAD_REQUEST.getStatusCode());
        }
        recordType.setId(id);

        ImportResult<RecordType> result;
        try {
            result = RecordTypeImport.importRecordType(recordType, ImportMode.UPDATE, IdentificationMode.ID,
                    null, repository.getTypeManager());
        } catch (RepositoryException e) {
            throw new ResourceException("Error creating or updating record type with id " + id, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }

        recordType = result.getEntity();
        Response response;

        ImportResultType resultType = result.getResultType();
        switch (resultType) {
            case UPDATED:
            case UP_TO_DATE:
                response = Response.ok(recordType).build();
                break;
            case CANNOT_UPDATE_DOES_NOT_EXIST:
                throw new ResourceException("Record type not found: " + id, NOT_FOUND.getStatusCode());
            default:
                throw new RuntimeException("Unexpected import result type: " + resultType);
        }

        return response;
    }

}

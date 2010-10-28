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
import org.lilyproject.util.repo.VersionTag;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.util.Collections;
import java.util.List;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("record/{id}/vtag/{vtag}")
public class RecordByVtagResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public Record get(@PathParam("id") String id, @PathParam("vtag") String vtag, @Context UriInfo uriInfo) {
        QName vtagName = new QName(VersionTag.NAMESPACE, vtag);
        List<QName> fieldQNames = ResourceClassUtil.parseFieldList(uriInfo);

        RecordId recordId = repository.getIdGenerator().fromString(id);
        Record record;
        try {
            // First read record with its vtags
            try {
                record = repository.read(recordId, Collections.singletonList(vtagName));
            } catch (FieldTypeNotFoundException e) {
                // We assume this is because the vtag field type could not be found
                // (no other field types should be loaded, so this is the only one this exception should
                //  be thrown for)
                throw new ResourceException(e, NOT_FOUND.getStatusCode());
            }

            if (!record.hasField(vtagName)) {
                throw new ResourceException("Record does not have a vtag " + vtagName, NOT_FOUND.getStatusCode());
            }

            long version = (Long)record.getField(vtagName);

            record = repository.read(recordId, version, fieldQNames);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return record;

    }

}

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

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import java.util.List;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("record/{id}/version")
public class RecordVersionCollectionResource extends RepositoryEnabled {

    @GET
    @Produces("application/json")
    public EntityList<Record> get(@PathParam("id") String id,
            @DefaultValue("1") @QueryParam("start-index") Long startIndex,
            @DefaultValue("10") @QueryParam("max-results") Long maxResults,
            @Context UriInfo uriInfo) {

        List<QName> fieldQNames = ResourceClassUtil.parseFieldList(uriInfo);

        RecordId recordId = repository.getIdGenerator().fromString(id);
        List<Record> records;
        try {
            records = repository.readVersions(recordId, startIndex, startIndex + maxResults - 1, fieldQNames);
            return new EntityList<Record>(records);
        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record versions.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}

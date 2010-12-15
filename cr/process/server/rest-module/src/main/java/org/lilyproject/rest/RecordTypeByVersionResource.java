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

import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.TypeException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("schema/recordType/{name}/version/{version:\\d+}")
public class RecordTypeByVersionResource extends RepositoryEnabled {
    @GET
    @Produces("application/json")
    public RecordType get(@PathParam("name") String name, @PathParam("version") Long version, @Context UriInfo uriInfo) {
        QName qname = ResourceClassUtil.parseQName(name, uriInfo.getQueryParameters());
        try {
            return repository.getTypeManager().getRecordTypeByName(qname, version);
        } catch (RecordTypeNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (Exception e) {
            throw new ResourceException("Error loading record type with name " + qname + ", version " + version, e,
                    INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}

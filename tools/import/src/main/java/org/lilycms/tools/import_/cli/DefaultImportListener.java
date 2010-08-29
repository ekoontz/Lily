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
package org.lilycms.tools.import_.cli;

import java.io.PrintStream;

public class DefaultImportListener implements ImportListener {
    private PrintStream out;

    public DefaultImportListener() {
        this(System.out);
    }

    public DefaultImportListener(PrintStream out) {
        this.out = out;
    }

    public void conflict(EntityType entityType, String entityName, String propName, Object oldValue, Object newValue)
            throws ImportConflictException {
        throw new ImportConflictException(String.format("%1$s %2$s exists but with %3$s %4$s instead of %5$s",
                toText(entityType), entityName, propName, oldValue, newValue));
    }

    public void existsAndEqual(EntityType entityType, String entityName, String entityId) {
        out.println(String.format("%1$s already exists and is equal: %2$s", toText(entityType), id(entityName, entityId)));
    }

    public void updated(EntityType entityType, String entityName, String entityId, long version) {
        if (entityType == EntityType.RECORD || entityType == EntityType.RECORD_TYPE) {
            out.println(String.format("%1$s updated: %2$s (version %3$s)", toText(entityType), id(entityName, entityId), version));
        } else {
            out.println(String.format("%1$s updated: %2$s", toText(entityType), id(entityName, entityId)));
        }
    }

    public void created(EntityType entityType, String entityName, String entityId) {
        out.println(String.format("%1$s created: %2$s", toText(entityType), id(entityName, entityId)));
    }

    private String id(String entityName, String entityId) {
        if (entityName != null) {
            return entityName;
        } else {
            return entityId;
        }
    }

    private String toText(EntityType entityType) {
        String entityTypeName;
        switch (entityType) {
            case FIELD_TYPE:
                entityTypeName = "Field type";
                break;
            case RECORD_TYPE:
                entityTypeName = "Record type";
                break;
            case RECORD:
                entityTypeName = "Record";
                break;
            default:
                throw new RuntimeException("Unexpected entity type: " + entityType);
        }
        return entityTypeName;
    }
}

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
package org.lilyproject.linkindex;

import org.lilyproject.repository.api.RecordId;

/**
 * A link to some record occurring in some field.
 */
public class FieldedLink {
    private final RecordId recordId;
    private final String fieldTypeId;
    private final int hash;

    public FieldedLink(RecordId recordId, String fieldTypeId){
        this.recordId = recordId;
        this.fieldTypeId = fieldTypeId;

        int hash = 17;
        hash = 37 * hash + recordId.toString().hashCode();
        hash = 37 * hash + fieldTypeId.hashCode();
        this.hash = hash;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public String getFieldTypeId() {
        return fieldTypeId;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FieldedLink) {
            FieldedLink otherLink = (FieldedLink)obj;
            return recordId.equals(otherLink.recordId) && fieldTypeId.equals(otherLink.fieldTypeId);
        }
        return false;
    }
}

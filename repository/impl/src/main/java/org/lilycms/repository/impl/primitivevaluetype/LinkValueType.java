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
package org.lilycms.repository.impl.primitivevaluetype;

import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.Link;
import org.lilycms.repository.api.PrimitiveValueType;
import org.lilycms.repository.api.RecordId;

/**
 *
 */
public class LinkValueType implements PrimitiveValueType {
    
    private final String NAME = "LINK";
    private final IdGenerator idGenerator;

    public LinkValueType(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
        
    }
    
    public String getName() {
        return NAME;
    }

    public Link fromBytes(byte[] bytes) {
        return Link.fromBytes(bytes, idGenerator);
    }

    public byte[] toBytes(Object value) {
        return ((Link)value).toBytes();
    }

    public Class getType() {
        return RecordId.class;
    }
}

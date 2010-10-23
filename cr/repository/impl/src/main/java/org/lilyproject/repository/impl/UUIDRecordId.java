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
package org.lilyproject.repository.impl;

import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.RecordId;

public class UUIDRecordId implements RecordId {

    private UUID uuid;
    private final IdGeneratorImpl idGenerator;

    private static final SortedMap<String, String> EMPTY_SORTED_MAP = Collections.unmodifiableSortedMap(new TreeMap<String, String>());

    protected UUIDRecordId(IdGeneratorImpl idGenerator) {
        this.idGenerator = idGenerator;
        uuid = UUID.randomUUID();
    }
    
    protected UUIDRecordId(long mostSigBits, long leastSigBits, IdGeneratorImpl idGenerator) {
        this.idGenerator = idGenerator;
        this.uuid = new UUID(mostSigBits, leastSigBits);
    }

    public UUIDRecordId(String uuidString, IdGeneratorImpl idgenerator) {
        this.idGenerator = idgenerator;
        this.uuid = UUID.fromString(uuidString);
    }
    
    public UUID getUuid() {
        return uuid;
    }
    
    public String toString() {
        return idGenerator.toString(this);
    }
    
    public byte[] toBytes() {
        return idGenerator.toBytes(this);
    }

    public SortedMap<String, String> getVariantProperties() {
        return EMPTY_SORTED_MAP;
    }

    protected byte[] getBasicBytes() {
        byte[] bytes = new byte[16];
        Bytes.putLong(bytes, 0, uuid.getMostSignificantBits());
        Bytes.putLong(bytes, 8, uuid.getLeastSignificantBits());
        return bytes;
    }
    
    protected String getBasicString() {
        return uuid.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UUIDRecordId other = (UUIDRecordId) obj;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
            return false;
        return true;
    }

    public RecordId getMaster() {
        return this;
    }

    public boolean isMaster() {
        return true;
    }
}

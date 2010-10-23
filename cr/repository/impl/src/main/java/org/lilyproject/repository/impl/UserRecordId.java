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

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.RecordId;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;


public class UserRecordId implements RecordId {

    protected final String recordIdString;
    protected byte[] recordIdBytes;
    private final IdGeneratorImpl idGenerator;
    
    private static final SortedMap<String, String> EMPTY_SORTED_MAP = Collections.unmodifiableSortedMap(new TreeMap<String, String>());

    protected UserRecordId(String recordId, IdGeneratorImpl idGenerator) {
        this.recordIdString = recordId;
        recordIdBytes = Bytes.toBytes(recordId);
        this.idGenerator = idGenerator;
    }

    protected UserRecordId(byte[] recordId, IdGeneratorImpl idGenerator) {
        recordIdBytes = recordId;
        recordIdString = Bytes.toString(recordId);
        IdGeneratorImpl.checkIdString(recordIdString, "record id");
        this.idGenerator = idGenerator;
    }

    public byte[] toBytes() {
        return idGenerator.toBytes(this);
    }

    public String toString() {
        return idGenerator.toString(this);
    }
    
    protected byte[] getBasicBytes() {
        return recordIdBytes;
    }
    
    protected String getBasicString() {
        return recordIdString;
    }

    public SortedMap<String, String> getVariantProperties() {
        return EMPTY_SORTED_MAP;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((recordIdString == null) ? 0 : recordIdString.hashCode());
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
        UserRecordId other = (UserRecordId) obj;
        if (recordIdString == null) {
            if (other.recordIdString != null)
                return false;
        } else if (!recordIdString.equals(other.recordIdString))
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

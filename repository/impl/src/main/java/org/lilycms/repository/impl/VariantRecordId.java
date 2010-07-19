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
package org.lilycms.repository.impl;

import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.RecordId;

public class VariantRecordId implements RecordId {

    private final RecordId masterRecordId;
    private final SortedMap<String, String> variantProperties;
    private final IdGeneratorImpl idGenerator;


    protected VariantRecordId(RecordId masterRecordId, Map<String, String> variantProperties, IdGeneratorImpl idGenerator) {
        this.masterRecordId = masterRecordId;

        SortedMap<String, String> varProps = createVariantPropertiesMap();
        varProps.putAll(variantProperties);
        this.variantProperties = Collections.unmodifiableSortedMap(varProps);

        this.idGenerator = idGenerator;
    }
    
    /**
     * The bytes of the masterRecordId are appended with the bytes of the variantProperties
     * The variantProperties bytes ends with an integer indicating the total length of the bytes
     * Each dimension and dimensionValue starts with an integer indicating the size of the dimension or dimensionValue
     */
    protected VariantRecordId(byte[] variantRecordIdBytes, IdGeneratorImpl idGenerator) {
        this.idGenerator = idGenerator;

        int variantPropertyLength = Bytes.toInt(variantRecordIdBytes, variantRecordIdBytes.length-Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        this.masterRecordId = idGenerator.fromBytes(Bytes.head(variantRecordIdBytes, variantRecordIdBytes.length-variantPropertyLength-Bytes.SIZEOF_INT));

        SortedMap<String, String> varProps = createVariantPropertiesMap();
        int offset = variantRecordIdBytes.length - variantPropertyLength - Bytes.SIZEOF_INT;
        while (offset < variantRecordIdBytes.length - Bytes.SIZEOF_INT) {
            int dimensionLength = Bytes.toInt(variantRecordIdBytes, offset, Bytes.SIZEOF_INT);
            offset = offset + Bytes.SIZEOF_INT;
            String dimension = Bytes.toString(variantRecordIdBytes, offset, dimensionLength);
            offset = offset + dimensionLength;
            int dimensionValueLength = Bytes.toInt(variantRecordIdBytes, offset, Bytes.SIZEOF_INT);
            offset = offset + Bytes.SIZEOF_INT;
            String dimensionValue = Bytes.toString(variantRecordIdBytes, offset, dimensionValueLength);

            IdGeneratorImpl.checkVariantPropertyNameValue(dimension);
            IdGeneratorImpl.checkVariantPropertyNameValue(dimensionValue);

            varProps.put(dimension, dimensionValue);
            offset = offset + dimensionValueLength;
        }
        this.variantProperties = Collections.unmodifiableSortedMap(varProps);
    }

    private SortedMap<String, String> createVariantPropertiesMap() {
        // Make sure they are always sorted the same way
        return new TreeMap<String, String>();
    }

    public String toString() {
        return idGenerator.toString(this);
    }
    
    public byte[] toBytes() {
        return idGenerator.toBytes(this);
    }

    protected byte[] getBasicBytes() {
        byte[] masterRecordIdBytes = masterRecordId.toBytes();
        Set<Entry<String,String>> entrySet = variantProperties.entrySet();
        // TODO use nio ByteBuffer here?
        byte[] variantPropertyBytes = new byte[0];
        for (Entry<String, String> entry : entrySet) {
            byte[] dimensionBytes = Bytes.toBytes(entry.getKey());
            byte[] dimensionValueBytes = Bytes.toBytes(entry.getValue());
            // TODO use short instead of int for length?
            variantPropertyBytes = Bytes.add(variantPropertyBytes, Bytes.toBytes(dimensionBytes.length));
            variantPropertyBytes = Bytes.add(variantPropertyBytes, dimensionBytes);
            variantPropertyBytes = Bytes.add(variantPropertyBytes, Bytes.toBytes(dimensionValueBytes.length));
            variantPropertyBytes = Bytes.add(variantPropertyBytes, dimensionValueBytes);
        }
        variantPropertyBytes = Bytes.add(variantPropertyBytes, Bytes.toBytes(variantPropertyBytes.length));
        return Bytes.add(masterRecordIdBytes, variantPropertyBytes);
    }
    
    public RecordId getMaster() {
        return masterRecordId;
    }

    public boolean isMaster() {
        return false;
    }

    public SortedMap<String, String> getVariantProperties() {
        return variantProperties;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((masterRecordId == null) ? 0 : masterRecordId.hashCode());
        result = prime * result + ((variantProperties == null) ? 0 : variantProperties.hashCode());
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
        VariantRecordId other = (VariantRecordId) obj;
        if (masterRecordId == null) {
            if (other.masterRecordId != null)
                return false;
        } else if (!masterRecordId.equals(other.masterRecordId))
            return false;
        if (variantProperties == null) {
            if (other.variantProperties != null)
                return false;
        } else if (!variantProperties.equals(other.variantProperties))
            return false;
        return true;
    }
}

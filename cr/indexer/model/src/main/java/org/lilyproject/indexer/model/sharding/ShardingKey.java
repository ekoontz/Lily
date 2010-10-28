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
package org.lilyproject.indexer.model.sharding;

import org.lilyproject.repository.api.RecordId;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ShardingKey {
    private ShardingKeyValue value;

    /** True if hash should be calculated. */
    private boolean hash;

    /** If > 0, calculate modulus after hash. */
    private int modulus;

    private KeyType keyType;

    enum KeyType { STRING, LONG }

    private ShardingKey(ShardingKeyValue value, boolean hash, int modulus, KeyType keyType) {
        this.value = value;
        this.hash = hash;
        this.modulus = modulus;
        this.keyType = keyType;
    }

    public static ShardingKey recordIdShardingKey(boolean hash, int modulus, KeyType keyType) {
        return new ShardingKey(new RecordIdShardingKeyValue(), hash, modulus, keyType);
    }

    public static ShardingKey masterRecordIdShardingKey(boolean hash, int modulus, KeyType keyType) {
        return new ShardingKey(new MasterRecordIdShardingKeyValue(), hash, modulus, keyType);
    }

    public static ShardingKey variantProperyShardingKey(String propertyName, boolean hash, int modulus, KeyType keyType) {
        return new ShardingKey(new VariantPropertyShardingKeyValue(propertyName), hash, modulus, keyType);
    }

    public Comparable getShardingKey(RecordId recordId) throws ShardSelectorException {
        Object key = value.getValue(recordId);

        if (hash) {
            long hash = hash(key.toString());

            key = hash;
        }

        switch (keyType) {
            case STRING:
                key = key.toString();
                break;
            case LONG:
                if (key instanceof Number) {
                    key = ((Number)key).longValue();
                } else {
                    try {
                        key = Long.parseLong(key.toString());
                    } catch (NumberFormatException e) {
                        throw new ShardSelectorException("Error parsing sharding key as long. Value: " + key, e);
                    }
                }
                break;
        }

        if (modulus > 0) {
            key = ((Long)key) % modulus;
        }

        return (Comparable)key;
    }

    private static long hash(String key) throws ShardSelectorException {
        try {
            MessageDigest mdAlgorithm = MessageDigest.getInstance("MD5");
            mdAlgorithm.update(key.getBytes("UTF-8"));
            byte[] digest = mdAlgorithm.digest();
            return ((digest[0] & 0xFF) << 8) + ((digest[1] & 0xFF));
        } catch (NoSuchAlgorithmException e) {
            throw new ShardSelectorException("Error calculating hash.", e);
        } catch (UnsupportedEncodingException e) {
            throw new ShardSelectorException("Error calculating hash.", e);
        }
    }

    private interface ShardingKeyValue {
        String getValue(RecordId recordId) throws ShardSelectorException;
    }

    private static class RecordIdShardingKeyValue implements ShardingKeyValue {
        public String getValue(RecordId recordId) {
            return recordId.toString();
        }
    }

    private static class MasterRecordIdShardingKeyValue implements ShardingKeyValue {
        public String getValue(RecordId recordId) {
            return recordId.getMaster().toString();
        }
    }

    private static class VariantPropertyShardingKeyValue implements ShardingKeyValue {
        private String propertyName;

        public VariantPropertyShardingKeyValue(String propertyName) {
            this.propertyName = propertyName;
        }

        public String getValue(RecordId recordId) throws ShardSelectorException {
            String propertyValue = recordId.getVariantProperties().get(propertyName);
            if (propertyValue == null) {
                throw new ShardSelectorException("Variant property used for sharding has no value. Property " +
                        propertyName + " in record id: " + recordId);
            }
            return propertyValue;
        }
    }
}

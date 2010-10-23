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
package org.lilyproject.repository.impl.primitivevaluetype;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.PrimitiveValueType;

public class DecimalValueType implements PrimitiveValueType {

    private final String NAME = "DECIMAL";

    public String getName() {
        return NAME;
    }

    public BigDecimal fromBytes(byte[] bytes) {
        return new BigDecimal(Bytes.toString(bytes));
    }

    public byte[] toBytes(Object value) {
        return Bytes.toBytes(((BigDecimal)value).toString());
    }

    public Class getType() {
        return Long.class;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((NAME == null) ? 0 : NAME.hashCode());
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
        DecimalValueType other = (DecimalValueType) obj;
        if (NAME == null) {
            if (other.NAME != null)
                return false;
        } else if (!NAME.equals(other.NAME))
            return false;
        return true;
    }
}

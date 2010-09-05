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
package org.lilycms.indexer.test;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.lilycms.indexer.formatter.DefaultFormatter;
import org.lilycms.repository.api.ValueType;

import java.util.HashSet;
import java.util.Set;

public class DateYearFormatter extends DefaultFormatter {
    private static Set<String> types;
    static {
        types = new HashSet<String>();
        types.add("DATE");
        types.add("DATETIME");
    }

    @Override
    protected String formatPrimitiveValue(Object value, ValueType valueType) {
        String type = valueType.getPrimitive().getName();
        if (type.equals("DATE")) {
            LocalDate date = (LocalDate)value;
            return String.valueOf(date.getYear());
        } else if (type.equals("DATETIME")) {
            DateTime dateTime = (DateTime)value;
            return String.valueOf(dateTime.getYear());
        } else {
            throw new RuntimeException("Unexpected type: " + type);
        }
    }

    @Override
    public Set<String> getSupportedPrimitiveValueTypes() {
        return types;
    }

}

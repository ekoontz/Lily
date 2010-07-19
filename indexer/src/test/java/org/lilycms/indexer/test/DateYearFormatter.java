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

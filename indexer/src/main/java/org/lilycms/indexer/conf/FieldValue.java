package org.lilycms.indexer.conf;

import org.lilycms.repository.api.*;

import java.util.Collections;
import java.util.List;

public class FieldValue extends BaseValue {
    private FieldType fieldType;

    protected FieldValue(FieldType fieldType, boolean extractContent, String formatterName, Formatters formatters) {
        super(extractContent, formatterName, formatters);
        this.fieldType = fieldType;
    }

    @Override
    public Object evalInt(IdRecord record, Repository repository, String vtag) {
        try {
            return record.getField(fieldType.getId());
        } catch (FieldNotFoundException e) {
            // TODO
            return null;
        }
    }

    @Override
    public ValueType getValueType() {
        return fieldType.getValueType();
    }

    public String getFieldDependency() {
        return fieldType.getId();
    }

    @Override
    public FieldType getTargetFieldType() {
        return fieldType;
    }
}

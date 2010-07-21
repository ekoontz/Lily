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
package org.lilycms.indexer.conf;

import org.lilycms.repository.api.*;
import org.lilycms.util.repo.VersionTag;

import java.util.*;

public class DerefValue extends BaseValue {
    private List<Follow> follows = new ArrayList<Follow>();
    private FieldType fieldType;
    private ValueType valueType;

    protected DerefValue(FieldType fieldType, boolean extractContent, String formatterName, Formatters formatters) {
        super(extractContent, formatterName, formatters);
        this.fieldType = fieldType;
    }

    /**
     * This method should be called after all follow-expressions have been added.
     */
    protected void init(TypeManager typeManager) {
        // In case the deref field itself is not multi-valued, but one of the follow-fields is multivalued,
        // then the value type of this Value is adjusted to to be multi-valued.

        if (fieldType.getValueType().isMultiValue()) {
            this.valueType = fieldType.getValueType();
            return;
        }

        boolean multiValue = false;
        for (Follow follow : follows) {
            if (follow.isMultiValue()) {
                multiValue = true;
                break;
            }
        }

        if (multiValue) {
            this.valueType = typeManager.getValueType(fieldType.getValueType().getPrimitive().getName(), true,
                    fieldType.getValueType().isHierarchical());
        } else {
            this.valueType = fieldType.getValueType();
        }
    }

    protected void addFieldFollow(FieldType fieldType) {
        follows.add(new FieldFollow(fieldType));
    }

    protected void addMasterFollow() {
        follows.add(new MasterFollow());
    }

    protected void addVariantFollow(Set<String> dimensions) {
        follows.add(new VariantFollow(dimensions));
    }

    public List<Follow> getFollows() {
        return Collections.unmodifiableList(follows);
    }

    /**
     * Returns the field taken from the document to which the follow-expressions point, thus the last
     * field in the chain.
     */
    public FieldType getTargetField() {
        return fieldType;
    }

    public static interface Follow {
        List<IdRecord> eval(IdRecord record, Repository repository, String vtag);

        boolean isMultiValue();
    }

    public static class FieldFollow implements Follow {
        FieldType fieldType;

        public FieldFollow(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        public boolean isMultiValue() {
            return fieldType.getValueType().isMultiValue();
        }

        public List<IdRecord> eval(IdRecord record, Repository repository, String vtag) {
            if (!record.hasField(fieldType.getId())) {
                return null;
            }

            if (vtag.equals(VersionTag.VERSIONLESS_TAG) && fieldType.getScope() != Scope.NON_VERSIONED) {
                // From a versionless record, it is impossible to deref a versioned field.
                // This explicit check could be removed if in case of the versionless vtag we only read
                // the non-versioned fields of the record. However, it is not possible to do this right
                // now with the repository API.
                return null;
            }

            Object value = record.getField(fieldType.getId());
            if (value instanceof Link) {
                RecordId recordId = ((Link)value).resolve(record, repository.getIdGenerator());
                IdRecord linkedRecord = resolveRecordId(recordId, vtag, repository);
                return linkedRecord == null ? null : Collections.singletonList(linkedRecord);
            } else if (value instanceof List && ((List)value).size() > 0 && ((List)value).get(0) instanceof Link) {
                List list = (List)value;
                List<IdRecord> result = new ArrayList<IdRecord>(list.size());
                for (Object link : list) {
                    RecordId recordId = ((Link)link).resolve(record, repository.getIdGenerator());
                    IdRecord linkedRecord = resolveRecordId(recordId, vtag, repository);
                    if (linkedRecord != null) {
                        result.add(linkedRecord);
                    }
                }
                return list.isEmpty() ? null : result;
            }
            return null;
        }

        public String getFieldId() {
            return fieldType.getId();
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        private IdRecord resolveRecordId(RecordId recordId, String vtag, Repository repository) {
            try {
                // TODO we could limit this to only load the field necessary for the next follow
                return VersionTag.getIdRecord(recordId, vtag, repository);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public static class MasterFollow implements Follow {
        public List<IdRecord> eval(IdRecord record, Repository repository, String vtag) {
            if (record.getId().isMaster())
                return null;

            RecordId masterId = record.getId().getMaster();

            try {
                IdRecord master = VersionTag.getIdRecord(masterId, vtag, repository);
                return master == null ? null : Collections.singletonList(master);
            } catch (Exception e) {
                return null;
            }
        }

        public boolean isMultiValue() {
            return false;
        }
    }

    public static class VariantFollow implements Follow {
        private Set<String> dimensions;

        public VariantFollow(Set<String> dimensions) {
            this.dimensions = dimensions;
        }

        public List<IdRecord> eval(IdRecord record, Repository repository, String vtag) {
            RecordId recordId = record.getId();

            Map<String, String> varProps = new HashMap<String, String>(recordId.getVariantProperties());

            for (String dimension : dimensions) {
                if (!varProps.containsKey(dimension)) {
                    return null;
                }
                varProps.remove(dimension);
            }

            RecordId resolvedRecordId = repository.getIdGenerator().newRecordId(recordId.getMaster(), varProps);

            try {
                IdRecord lessDimensionedRecord = VersionTag.getIdRecord(resolvedRecordId, vtag, repository);
                return lessDimensionedRecord == null ? null : Collections.singletonList(lessDimensionedRecord);
            } catch (Exception e) {
                return null;
            }
        }

        public Set<String> getDimensions() {
            return dimensions;
        }

        public boolean isMultiValue() {
            return false;
        }
    }

    @Override
    public Object evalInt(IdRecord record, Repository repository, String vtag) {
        if (vtag.equals(VersionTag.VERSIONLESS_TAG) && fieldType.getScope() != Scope.NON_VERSIONED) {
            // From a versionless record, it is impossible to deref a versioned field.
            return null;
        }

        List<IdRecord> records = new ArrayList<IdRecord>();
        records.add(record);

        for (Follow follow : follows) {
            List<IdRecord> linkedRecords = new ArrayList<IdRecord>();

            for (IdRecord item : records) {
                List<IdRecord> evalResult = follow.eval(item, repository, vtag);
                if (evalResult != null) {
                    linkedRecords.addAll(evalResult);
                }
            }

            records = linkedRecords;
        }

        if (records.isEmpty())
            return null;

        List<Object> result = new ArrayList<Object>();
        for (IdRecord item : records) {
            if (item.hasField(fieldType.getId())) {
                Object value = item.getField(fieldType.getId());
                if (value != null) {
                    result.add(value);
                }
            }
        }

        if (result.isEmpty())
            return null;

        if (!valueType.isMultiValue())
            return result.get(0);

        return result;
    }

    @Override
    public ValueType getValueType() {
        return valueType;
    }

    public String getFieldDependency() {
        if (follows.get(0) instanceof FieldFollow) {
            return ((FieldFollow)follows.get(0)).fieldType.getId();
        } else {
            // A follow-variant is like a link to another document, but the link can never change as the
            // identity of the document never changes. Therefore, there is no dependency on a field.
            return null;
        }
    }

    @Override
    public FieldType getTargetFieldType() {
        return fieldType;
    }
}

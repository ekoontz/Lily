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
package org.lilycms.util.repo;

import org.apache.commons.logging.LogFactory;
import org.lilycms.repository.api.*;

import java.util.*;

/**
 * Version tag related utilities.
 */
public class VersionTag {

    /**
     * Namespace for field types that serve as version tags.
     */
    public static final String NAMESPACE = "org.lilycms.vtag";

    /**
     * A dummy tag used for documents which have no versions, and thus no tagged versions.
     */
    public static final String VERSIONLESS_TAG = "@@versionless";

    public static QName qname(String vtag) {
        return new QName(NAMESPACE, vtag);
    }

    /**
     * Returns the vtags of a record, the key in the map is the field type ID of the vtag field, not its name.
     *
     * <p>Note that version numbers do not necessarily correspond to existing versions.
     */
    public static Map<String, Long> getTagsById(Record record, TypeManager typeManager) {
        Map<String, Long> vtags = new HashMap<String, Long>();

        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeByName(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                // A field whose field type does not exist: skip it
                // TODO would be better to do above retrieval based on ID?
                continue;
            } catch (TypeException e) {
                // TODO maybe this should rather be thrown?
                continue;
            }

            if (isVersionTag(fieldType)) {
                vtags.put(fieldType.getId(), (Long)field.getValue());
            }
        }

        return vtags;
    }

    /**
     * Returns the vtags of a record, the key in the map is the field type ID of the vtag field, not its name.
     *
     * <p>Note that version numbers do not necessarily correspond to existing versions.
     */
    public static Map<String, Long> getTagsById(IdRecord record, TypeManager typeManager) {
        Map<String, Long> vtags = new HashMap<String, Long>();

        for (Map.Entry<String, Object> field : record.getFieldsById().entrySet()) {
            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeById(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                // A field whose field type does not exist: skip it
                continue;
            } catch (Exception e) {
                // Other problem loading field type: skip it
                // TODO log this also as an error
                continue;
            }

            if (isVersionTag(fieldType)) {
                vtags.put(fieldType.getId(), (Long)field.getValue());
            }
        }

        return vtags;
    }

    /**
     * Returns the vtags of a record, the key in the map is the name of the vtag field (without namespace).
     *
     * <p>Note that version numbers do not necessarily correspond to existing versions.
     */
    public static Map<String, Long> getTagsByName(Record record, TypeManager typeManager) {
        Map<String, Long> vtags = new HashMap<String, Long>();

        for (Map.Entry<QName, Object> field : record.getFields().entrySet()) {
            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeByName(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                // A field whose field type does not exist: skip it
                // TODO would be better to do above retrieval based on ID?
                continue;
            } catch (TypeException e) {
                // TODO maybe this should rather be thrown?
                continue;
            }

            if (isVersionTag(fieldType)) {
                vtags.put(fieldType.getName().getName(), (Long)field.getValue());
            }
        }

        return vtags;
    }

    /**
     * Returns true if the given FieldType is a version tag.
     */
    public static boolean isVersionTag(FieldType fieldType) {
        return (fieldType.getName().getNamespace().equals(NAMESPACE)
                && fieldType.getScope() == Scope.NON_VERSIONED
                && fieldType.getValueType().isPrimitive()
                && fieldType.getValueType().getPrimitive().getName().equals("LONG"));
    }

    /**
     * Inverts a map containing version by tag to a map containing tags by version. It does not matter if the
     * tags are identified by name or by ID.
     */
    public static Map<Long, Set<String>> tagsByVersion(Map<String, Long> vtags) {
        Map<Long, Set<String>> result = new HashMap<Long, Set<String>>();

        for (Map.Entry<String, Long> entry : vtags.entrySet()) {
            Set<String> tags = result.get(entry.getValue());
            if (tags == null) {
                tags = new HashSet<String>();
                result.put(entry.getValue(), tags);
            }
            tags.add(entry.getKey());
        }

        return result;
    }

    /**
     * Filters the given set of fields to only those that are vtag fields.
     */
    public static Set<String> filterVTagFields(Set<String> fieldIds, TypeManager typeManager) {
        Set<String> result = new HashSet<String>();
        for (String field : fieldIds) {
            try {
                if (VersionTag.isVersionTag(typeManager.getFieldTypeById(field))) {
                    result.add(field);
                }
            } catch (FieldTypeNotFoundException e) {
                // ignore, if it does not exist, it can't be a version tag
            } catch (Throwable t) {
                LogFactory.getLog(VersionTag.class).error("Error loading field type to find out if it is a vtag field.", t);
            }
        }
        return result;
    }

    /**
     * Resolves a vtag to a version number for some record.
     *
     * <p>It does not assume the vtag exists, is really a vtag field, etc.
     *
     * <p>It should not be called for the @@versionless tag, since that cannot be resolved to a version number.
     *
     * <p>If the specified record would not exist, you will get an {@link RecordTypeNotFoundException}.
     *
     * @return null if the vtag does not exist, if it is not a valid vtag field, if the record does not exist,
     *         or if the record fails to load.
     */
    public static Long getVersion(RecordId recordId, String vtagId, Repository repository) {
        IdRecord vtagRecord;
        try {
            vtagRecord = repository.readWithIds(recordId, null, Collections.singletonList(vtagId));
        } catch (Exception e) {
            return null;
        }

        FieldType fieldType;
        try {
            fieldType = repository.getTypeManager().getFieldTypeById(vtagId);
        } catch (FieldTypeNotFoundException e) {
            return null;
        } catch (TypeException e) {
            // TODO log this? or throw it?
            return null;
        }

        if (!VersionTag.isVersionTag(fieldType)) {
            return null;
        }

        if (!vtagRecord.hasField(vtagId))
            return null;

        return (Long)vtagRecord.getField(vtagId);
    }

    /**
     * Get the version of a record as specified by the version tag.
     *
     * <p>Returns null if the version tag would not exist or point to a non-existing version.
     *
     * <p>The @@versionless version tag is supported.
     */
    public static Record getRecord(RecordId recordId, String vtagId, Repository repository, List<QName> fieldNames)
            throws FieldTypeNotFoundException, RepositoryException, RecordNotFoundException,
            RecordTypeNotFoundException, VersionNotFoundException {
        if (vtagId.equals(VersionTag.VERSIONLESS_TAG)) {
            // TODO this should include an option to only read non-versioned-scoped data
            return repository.read(recordId);
        } else {
            Long version = getVersion(recordId, vtagId, repository);
            if (version == null) {
                return null;
            }

            return repository.read(recordId, version, fieldNames);
        }
    }

    /**
     * See {@link #getRecord(org.lilycms.repository.api.RecordId, String, org.lilycms.repository.api.Repository, java.util.List)}.
     */
    public static Record getRecord(RecordId recordId, String vtagId, Repository repository)
            throws FieldTypeNotFoundException, RepositoryException, RecordNotFoundException,
            RecordTypeNotFoundException, VersionNotFoundException {
        return getRecord(recordId, vtagId, repository, null);
    }

    public static IdRecord getIdRecord(RecordId recordId, String vtagId, Repository repository)
            throws FieldTypeNotFoundException, RepositoryException, RecordNotFoundException,
            RecordTypeNotFoundException, VersionNotFoundException {
        return getIdRecord(recordId, vtagId, repository, null);
    }

    public static IdRecord getIdRecord(RecordId recordId, String vtagId, Repository repository, List<String> fieldIds)
            throws FieldTypeNotFoundException, RepositoryException, RecordNotFoundException,
            RecordTypeNotFoundException, VersionNotFoundException {
        if (vtagId.equals(VersionTag.VERSIONLESS_TAG)) {
            // TODO this should include an option to only read non-versioned-scoped data
            return repository.readWithIds(recordId, null, null);
        } else {
            Long version = getVersion(recordId, vtagId, repository);
            if (version == null) {
                return null;
            }

            return repository.readWithIds(recordId, version, fieldIds);
        }
    }
}

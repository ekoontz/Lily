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
package org.lilyproject.indexer.engine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.lilyproject.indexer.model.indexerconf.DerefValue;
import org.lilyproject.indexer.model.indexerconf.DerefValue.Follow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.FieldFollow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.VariantFollow;
import org.lilyproject.indexer.model.indexerconf.DerefValue.MasterFollow;
import org.lilyproject.indexer.model.indexerconf.FieldValue;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.Value;
import org.lilyproject.indexer.model.indexerconf.Formatter;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.VersionTag;

import java.io.InputStream;
import java.util.*;

/**
 * Evaluates an index field value (a {@link Value}) to a value.
 */
public class ValueEvaluator {
    private Log log = LogFactory.getLog(getClass());

    private IndexerConf conf;

    private Parser tikaParser = new AutoDetectParser();

    public ValueEvaluator(IndexerConf conf) {
        this.conf = conf;
    }

    /**
     * Evaluates a value for a given record & vtag.
     *
     * @return null if there is no value
     */
    public List<String> eval(Value valueDef, IdRecord record, Repository repository, String vtag) {
        Object value = evalValue(valueDef, record, repository, vtag);
        if (value == null)
            return null;

        if (valueDef.extractContent()) {
            return extractContent(valueDef, value, record, repository);
        }

        ValueType valueType = valueDef.getValueType();
        Formatter formatter = valueDef.getFormatter() != null ? conf.getFormatters().getFormatter(valueDef.getFormatter()) : conf.getFormatters().getFormatter(valueType);

        return formatter.format(value, valueType);
    }

    private List<String> extractContent(Value valueDef, Object value, IdRecord record, Repository repository) {
        // At this point we can be sure the value will be a blob, this is validated during
        // the construction of the indexer conf.

        ValueType valueType = valueDef.getValueType();

        List<Blob> blobs = new ArrayList<Blob>();
        collectBlobs(value, valueType, blobs);

        if (blobs.size() == 0)
            return null;

        List<String> result = new ArrayList<String>(blobs.size());

        // TODO add some debug (or even info) logging to indicate what we are working on.
        for (Blob blob : blobs) {
            InputStream is = null;
            try {
                is = repository.getInputStream(blob);

                // TODO make write limit configurable
                BodyContentHandler ch = new BodyContentHandler();

                Metadata metadata = new Metadata();
                metadata.add(Metadata.CONTENT_TYPE, blob.getMediaType());
                if (blob.getName() != null)
                    metadata.add(Metadata.RESOURCE_NAME_KEY, blob.getName());

                ParseContext parseContext = new ParseContext();

                tikaParser.parse(is, ch, metadata, parseContext);

                String text = ch.toString();
                if (text.length() > 0)
                    result.add(text);

            } catch (Throwable t) {
                log.error("Error extracting blob content. Field: " + valueDef.getTargetFieldType().getName() + ", record: "
                        + record.getId(), t);
            } finally {
                Closer.close(is);
            }
        }

        return result.isEmpty() ? null : result;
    }

    private void collectBlobs(Object value, ValueType valueType, List<Blob> blobs) {
        if (valueType.isMultiValue()) {
            List values = (List)value;
            for (Object item : values)
                collectBlobsHierarchical(item, valueType, blobs);
        } else {
            collectBlobsHierarchical(value, valueType, blobs);
        }
    }

    private void collectBlobsHierarchical(Object value, ValueType valueType, List<Blob> blobs) {
        if (valueType.isHierarchical()) {
            HierarchyPath hierarchyPath = (HierarchyPath)value;
            for (Object item : hierarchyPath.getElements())
                blobs.add((Blob)item);
        } else {
            blobs.add((Blob)value);
        }
    }

    private Object evalValue(Value value, IdRecord record, Repository repository, String vtag) {
        if (value instanceof FieldValue) {
            return evalFieldValue((FieldValue)value, record, repository, vtag);
        } else if (value instanceof DerefValue) {
            return evalDerefValue((DerefValue)value, record, repository, vtag);
        } else {
            throw new RuntimeException("Unexpected type of value: " + value.getClass().getName());
        }
    }

    private Object evalFieldValue(FieldValue value, IdRecord record, Repository repository, String vtag) {
        try {
            return record.getField(value.getFieldType().getId());
        } catch (FieldNotFoundException e) {
            // TODO
            return null;
        }
    }

    private Object evalDerefValue(DerefValue deref, IdRecord record, Repository repository, String vtag) {
        FieldType fieldType = deref.getTargetFieldType();
        if (vtag.equals(VersionTag.VERSIONLESS_TAG) && fieldType.getScope() != Scope.NON_VERSIONED) {
            // From a versionless record, it is impossible to deref a versioned field.
            return null;
        }

        List<IdRecord> records = new ArrayList<IdRecord>();
        records.add(record);

        for (Follow follow : deref.getFollows()) {
            List<IdRecord> linkedRecords = new ArrayList<IdRecord>();

            for (IdRecord item : records) {
                List<IdRecord> evalResult = evalFollow(deref, follow, item, repository, vtag);
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
                    if (deref.getTargetField().getValueType().isMultiValue()) {
                        result.addAll((List)value);
                    } else {
                        result.add(value);
                    }
                }
            }
        }

        if (result.isEmpty())
            return null;

        if (!deref.getValueType().isMultiValue())
            return result.get(0);

        return result;
    }

    private List<IdRecord> evalFollow(DerefValue deref, Follow follow, IdRecord record, Repository repository, String vtag) {
        if (follow instanceof FieldFollow) {
            return evalFieldFollow(deref, (FieldFollow)follow, record, repository, vtag);
        } else if (follow instanceof VariantFollow) {
            return evalVariantFollow((VariantFollow)follow, record, repository, vtag);
        } else if (follow instanceof MasterFollow) {
            return evalMasterFollow((MasterFollow)follow, record, repository, vtag);
        } else {
            throw new RuntimeException("Unexpected type of follow: " + follow.getClass().getName());
        }
    }

    private List<IdRecord> evalFieldFollow(DerefValue deref, FieldFollow follow, IdRecord record, Repository repository, String vtag) {
        FieldType fieldType = follow.getFieldType();

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

    private IdRecord resolveRecordId(RecordId recordId, String vtag, Repository repository) {
        try {
            // TODO we could limit this to only load the field necessary for the next follow
            return VersionTag.getIdRecord(recordId, vtag, repository);
        } catch (Exception e) {
            return null;
        }
    }

    private List<IdRecord> evalVariantFollow(VariantFollow follow, IdRecord record, Repository repository, String vtag) {
        RecordId recordId = record.getId();

        Map<String, String> varProps = new HashMap<String, String>(recordId.getVariantProperties());

        for (String dimension : follow.getDimensions()) {
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

    private List<IdRecord> evalMasterFollow(MasterFollow follow, IdRecord record, Repository repository, String vtag) {
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
}

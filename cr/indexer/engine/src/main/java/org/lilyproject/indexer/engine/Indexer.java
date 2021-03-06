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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.indexerconf.IndexField;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.sharding.ShardSelectorException;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.repo.VersionTag;

import java.io.IOException;
import java.util.*;


// IMPORTANT: each call to solrServer should be followed by a corresponding metrics update.

/**
 * The Indexer adds records to, or removes records from, the index.
 */
public class Indexer {
    private IndexerConf conf;
    private Repository repository;
    private TypeManager typeManager;
    private SolrServers solrServers;
    private IndexLocker indexLocker;
    private ValueEvaluator valueEvaluator;
    private IndexerMetrics metrics;

    private Log log = LogFactory.getLog(getClass());

    public Indexer(IndexerConf conf, Repository repository, SolrServers solrServers, IndexLocker indexLocker,
            IndexerMetrics metrics) {
        this.conf = conf;
        this.repository = repository;
        this.solrServers = solrServers;
        this.indexLocker = indexLocker;
        this.typeManager = repository.getTypeManager();
        this.valueEvaluator = new ValueEvaluator(conf);
        this.metrics = metrics;
    }

    public IndexerConf getConf() {
        return conf;
    }

    /**
     * Performs a complete indexing of the given record, supposing the record is not yet indexed
     * (existing entries are not explicitly removed).
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     *
     * @param recordId
     */
    public void index(RecordId recordId) throws RepositoryException, IOException,
            SolrServerException, ShardSelectorException, InterruptedException {

        IdRecord record = repository.readWithIds(recordId, null, null);
        Map<String, Long> vtags = VersionTag.getTagsById(record, typeManager);

        IndexCase indexCase = conf.getIndexCase(record.getRecordTypeName(), record.getId().getVariantProperties());
        if (indexCase == null) {
            return;
        }

        Set<String> vtagsToIndex = new HashSet<String>();
        setIndexAllVTags(vtagsToIndex, vtags, indexCase, record);

        index(record, vtagsToIndex, vtags);
    }

    protected void index(IdRecord record, Set<String> vtagsToIndex, Map<String, Long> vtags)
            throws IOException, SolrServerException, RepositoryException, ShardSelectorException, InterruptedException {
        if (vtagsToIndex.contains(VersionTag.VERSIONLESS_TAG)) {
            // Usually when the @@versionless vtag should be indexed, the vtagsToIndex set will
            // not contain any other tags.
            // It could be that there are other tags however: for example if someone added and removed
            // vtag fields to the (versionless) document.
            // If we would ever support deleting of versions, then it could also be the case,
            // but then we'll have to extend this to delete these old versions from the index.
            index(record, Collections.singleton(VersionTag.VERSIONLESS_TAG));
        } else {
            indexRecord(record.getId(), vtagsToIndex, vtags);
        }
    }

    /**
     * Indexes a record for a set of vtags.
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     *
     * @param vtagsToIndex all vtags for which to index the record, not all vtags need to exist on the record,
     *                     but this should only contain appropriate vtags as defined by the IndexCase for this record.
     * @param vtags the actual vtag mappings of the record
     */
    protected void indexRecord(RecordId recordId, Set<String> vtagsToIndex, Map<String, Long> vtags)
            throws IOException, SolrServerException, FieldTypeNotFoundException, RepositoryException,
            RecordTypeNotFoundException, ShardSelectorException, InterruptedException {
        // One version might have multiple vtags, so to index we iterate the version numbers
        // rather than the vtags
        Map<Long, Set<String>> vtagsToIndexByVersion = getVtagsByVersion(vtagsToIndex, vtags);
        for (Map.Entry<Long, Set<String>> entry : vtagsToIndexByVersion.entrySet()) {
            IdRecord version = null;
            try {
                version = repository.readWithIds(recordId, entry.getKey(), null);
            } catch (VersionNotFoundException e) {
                // TODO
            } catch (RecordNotFoundException e) {
                // TODO handle this differently from version not found
            }

            if (version == null) {
                for (String vtag : entry.getValue()) {
                    verifyLock(recordId);
                    solrServers.getSolrServer(recordId).deleteById(getIndexId(recordId, vtag));
                    metrics.deletesById.inc();
                }

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, version %2$s: does not exist, deleted index" +
                            " entries for vtags %3$s", recordId, entry.getKey(), vtagSetToNameString(entry.getValue())));
                }
            } else {
                index(version, entry.getValue());
            }
        }
    }

    /**
     * The actual indexing: maps record fields to index fields, and send to SOLR.
     *
     * @param record the correct version of the record, which has the versionTag applied to it
     * @param vtags the version tags under which to index
     */
    protected void index(IdRecord record, Set<String> vtags) throws IOException, SolrServerException, ShardSelectorException {

        verifyLock(record.getId());

        // Note that it is important the the indexFields are evaluated in order, since multiple
        // indexFields can have the same name and the order of values for multi-value fields can be important.
        //
        // The value of the indexFields is re-evaluated for each vtag. It is only the value of
        // deref-values which can change from vtag to vtag, so we could optimize this by only
        // evaluating those after the first run, but again because we want to maintain order and
        // because a deref-field could share the same name with a non-deref field, we simply
        // re-evaluate all fields for each vtag.
        for (String vtag : vtags) {
            SolrInputDocument solrDoc = new SolrInputDocument();

            boolean valueAdded = false;
            for (IndexField indexField : conf.getIndexFields()) {
                List<String> values = valueEvaluator.eval(indexField.getValue(), record, repository, vtag);
                if (values != null) {
                    for (String value : values) {
                        solrDoc.addField(indexField.getName(), value);
                        valueAdded = true;
                    }
                }
            }

            if (!valueAdded) {
                // No single field was added to the SOLR document.
                // In this case we do not add it to the index.
                // Besides being somewhat logical, it should also be noted that if a record would not contain
                // any (modified) fields that serve as input to indexFields, we would never have arrived here
                // anyway. It is only because some fields potentially would resolve to a value (potentially:
                // because with deref-expressions we are never sure) that we did.

                // There can be a previous entry in the index which we should try to delete
                solrServers.getSolrServer(record.getId()).deleteById(getIndexId(record.getId(), vtag));
                metrics.deletesById.inc();
                
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s, vtag %2$s: no index fields produced output, " +
                            "removed from index if present", record.getId(), safeLoadTagName(vtag)));
                }

                continue;
            }


            solrDoc.setField("@@id", record.getId().toString());
            solrDoc.setField("@@key", getIndexId(record.getId(), vtag));
            solrDoc.setField("@@vtag", vtag);

            if (vtag.equals(VersionTag.VERSIONLESS_TAG)) {
                solrDoc.setField("@@versionless", "true");
            }

            solrServers.getSolrServer(record.getId()).add(solrDoc);
            metrics.adds.inc();

            if (log.isDebugEnabled()) {
                log.debug(String.format("Record %1$s, vtag %2$s: indexed", record.getId(), safeLoadTagName(vtag)));
            }
        }
    }

    /**
     * Deletes all index entries (for all vtags) for the given record.
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     */
    public void delete(RecordId recordId) throws IOException, SolrServerException, ShardSelectorException {
        verifyLock(recordId);
        solrServers.getSolrServer(recordId).deleteByQuery("@@id:" + ClientUtils.escapeQueryChars(recordId.toString()));
        metrics.deletesByQuery.inc();
    }

    /**
     *
     * <p>This method requires you obtained the {@link IndexLocker} for the record.
     */
    public void delete(RecordId recordId, String vtag) throws IOException, SolrServerException, ShardSelectorException {
        verifyLock(recordId);
        solrServers.getSolrServer(recordId).deleteById(getIndexId(recordId, vtag));
        metrics.deletesByQuery.inc();
    }

    private Map<Long, Set<String>> getVtagsByVersion(Set<String> vtagsToIndex, Map<String, Long> vtags) {
        Map<Long, Set<String>> result = new HashMap<Long, Set<String>>();

        for (String vtag : vtagsToIndex) {
            Long version = vtags.get(vtag);
            if (version != null) {
                Set<String> vtagsOfVersion = result.get(version);
                if (vtagsOfVersion == null) {
                    vtagsOfVersion = new HashSet<String>();
                    result.put(version, vtagsOfVersion);
                }
                vtagsOfVersion.add(vtag);
            }
        }

        return result;
    }

    protected void setIndexAllVTags(Set<String> vtagsToIndex, Map<String, Long> vtags, IndexCase indexCase, Record record) {
        if (record.getVersion() != null) {
            Set<String> tmp = new HashSet<String>();
            tmp.addAll(indexCase.getVersionTags());
            tmp.retainAll(vtags.keySet()); // only keep the vtags which exist in the document
            vtagsToIndex.addAll(tmp);
        } else if (indexCase.getIndexVersionless()) {
            vtagsToIndex.add(VersionTag.VERSIONLESS_TAG);
        }
    }

    protected String getIndexId(RecordId recordId, String vtag) {
        return recordId + "-" + vtag;
    }

    /**
     * Lookup name of field type, for use in debug logs. Beware, this might be slow.
     */
    protected String safeLoadTagName(String fieldTypeId) {
        if (fieldTypeId == null)
            return "null";
        if (fieldTypeId.equals(VersionTag.VERSIONLESS_TAG))
            return fieldTypeId;

        try {
            return typeManager.getFieldTypeById(fieldTypeId).getName().getName();
        } catch (Throwable t) {
            return "failed to load name";
        }
    }

    protected String vtagSetToNameString(Set<String> vtags) {
        StringBuilder builder = new StringBuilder();
        for (String vtag : vtags) {
            if (builder.length() > 0)
                builder.append(", ");
            builder.append(safeLoadTagName(vtag));
        }
        return builder.toString();
    }

    private void verifyLock(RecordId recordId) {
        try {
            if (!indexLocker.hasLock(recordId)) {
                throw new RuntimeException("Thread does not own index lock for record " + recordId);
            }
        } catch (Throwable t) {
            throw new RuntimeException("Error checking if we own index lock for record " + recordId);
        }
    }
}

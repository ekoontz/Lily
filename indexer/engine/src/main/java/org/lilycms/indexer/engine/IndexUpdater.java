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
package org.lilycms.indexer.engine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.*;
import org.lilycms.indexer.model.indexerconf.DerefValue;
import org.lilycms.indexer.model.indexerconf.IndexCase;
import org.lilycms.indexer.model.indexerconf.IndexField;
import org.lilycms.linkindex.LinkIndex;
import org.lilycms.repository.api.*;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.impl.RemoteListenerHandler;
import org.lilycms.util.repo.RecordEvent;
import org.lilycms.util.repo.VersionTag;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;
import org.lilycms.util.ObjectUtils;
import org.lilycms.util.zookeeper.ZooKeeperItf;

import static org.lilycms.util.repo.RecordEvent.Type.*;

import java.io.IOException;
import java.util.*;

/**
 * Updates the index in response to repository events.
 */
public class IndexUpdater {
    private RowLog rowLog;
    private String subscriptionId;
    private Repository repository;
    private TypeManager typeManager;
    private LinkIndex linkIndex;
    private RemoteListenerHandler listenerHandler;
    private IndexerListener indexerListener = new IndexerListener();
    private Indexer indexer;
    private IndexUpdaterMetrics metrics;
    private ClassLoader myContextClassLoader;
    private IndexLocker indexLocker;

    private Log log = LogFactory.getLog(getClass());

    public IndexUpdater(String subscriptionId, Indexer indexer, RowLog rowLog, Repository repository,
            LinkIndex linkIndex, IndexLocker indexLocker, ZooKeeperItf zk) throws RowLogException {
        this.indexer = indexer;
        this.rowLog = rowLog;
        this.subscriptionId = subscriptionId;
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.linkIndex = linkIndex;

        this.indexLocker = indexLocker;

        this.myContextClassLoader = Thread.currentThread().getContextClassLoader();

        this.metrics = new IndexUpdaterMetrics();


        listenerHandler = new RemoteListenerHandler(rowLog, subscriptionId, indexerListener, zk);
        listenerHandler.start();
    }

    public void stop() {
        listenerHandler.stop();
    }

    private class IndexerListener implements RowLogMessageListener {
        public boolean processMessage(RowLogMessage msg) {
            long before = System.currentTimeMillis();

            // During the processing of this message, we switch the context class loader to the one
            // of the Kauri module to which the index updater belongs. This is necessary for Tika
            // to find its parser implementations.

            ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(myContextClassLoader);
                RecordEvent event = new RecordEvent(msg.getPayload());
                RecordId recordId = repository.getIdGenerator().fromBytes(msg.getRowKey());

                if (log.isDebugEnabled()) {
                    log.debug("Received message: " + event.toJson());
                }

                if (event.getType().equals(DELETE)) {
                    // For deleted records, we cannot determine the record type, so we do not know if there was
                    // an applicable index case, so we always perform a delete.
                    indexLocker.lock(recordId);
                    try {
                        indexer.delete(recordId);
                    } finally {
                        indexLocker.unlockLogFailure(recordId);
                    }

                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Record %1$s: deleted from index (if present) because of " +
                                "delete record event", recordId));
                    }

                    // After this we can go to update denormalized data
                    updateDenormalizedData(recordId, event, null, null);
                } else {
                    Map<Long, Set<String>> vtagsByVersion;
                    Map<Scope, Set<FieldType>> updatedFieldsByScope;

                    indexLocker.lock(recordId);
                    try {
                        IdRecord record;
                        try {
                            record = repository.readWithIds(recordId, null, null);
                        } catch (RecordNotFoundException e) {
                            // The record has been deleted in the meantime.
                            // For now, we do nothing, when the delete event is received the record will be removed
                            // from the index.
                            // TODO: we should process all outstanding messages for the record (up to delete) in one go
                            return true;
                        }

                        // Read the vtags of the record. Note that while this algorithm is running, the record can meanwhile
                        // undergo changes. However, we continuously work with the snapshot of the vtags mappings read here.
                        // The processing of later events will bring the index up to date with any new changes.
                        Map<String, Long> vtags = VersionTag.getTagsById(record, typeManager);
                        vtagsByVersion = VersionTag.tagsByVersion(vtags);

                        updatedFieldsByScope = getFieldTypeAndScope(event.getUpdatedFields());

                        handleRecordCreateUpdate(record, event, vtags, vtagsByVersion, updatedFieldsByScope);
                    } finally {
                        indexLocker.unlockLogFailure(recordId);
                    }

                    updateDenormalizedData(recordId, event, updatedFieldsByScope, vtagsByVersion);
                }


            } catch (Exception e) {
                // TODO
                //  TODO if the error is an IndexLockException: indexing can be retried later thus rather return false
                log.error("Error processing event in indexer.", e);
            } finally {
                long after = System.currentTimeMillis();
                metrics.messageProcessed(after - before);
                Thread.currentThread().setContextClassLoader(currentCL);
            }
            return true;
        }
    }

    private void handleRecordCreateUpdate(IdRecord record, RecordEvent event, Map<String, Long> vtags,
            Map<Long, Set<String>> vtagsByVersion, Map<Scope, Set<FieldType>> updatedFieldsByScope) throws Exception {

        // Determine the IndexCase:
        //  The indexing of all versions is determined by the record type of the non-versioned scope.
        //  This makes that the indexing behavior of all versions is equal, and can be changed (the
        //  record type of the versioned scope is immutable).
        IndexCase indexCase = indexer.getConf().getIndexCase(record.getRecordTypeName(), record.getId().getVariantProperties());

        if (indexCase == null) {
            // The record should not be indexed
            // But data from this record might be denormalized into other index entries
            // After this we go to update denormalized data
        } else {
            Set<String> vtagsToIndex = new HashSet<String>();

            if (event.getType().equals(CREATE)) {
                // New record: just index everything
                indexer.setIndexAllVTags(vtagsToIndex, vtags, indexCase, record);
                // After this we go to the indexing

            } else if (event.getRecordTypeChanged()) {
                // When the record type changes, the rules to index (= the IndexCase) change

                // Delete everything: we do not know the previous record type, so we do not know what
                // version tags were indexed, so we simply delete everything
                indexer.delete(record.getId());

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Record %1$s: deleted existing entries from index (if present) " +
                            "because of record type change", record.getId()));
                }

                // Reindex all needed vtags
                indexer.setIndexAllVTags(vtagsToIndex, vtags, indexCase, record);

                // After this we go to the indexing
            } else { // a normal update

                if (event.getVersionCreated() == 1
                        && event.getType().equals(UPDATE)
                        && indexCase.getIndexVersionless()) {
                    // If the first version was created, but the record was not new, then there
                    // might already be an @@versionless index entry
                    indexer.delete(record.getId(), VersionTag.VERSIONLESS_TAG);

                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Record %1$s: deleted versionless entry from index (if present) " +
                                "because of creation first version", record.getId()));
                    }
                }

                //
                // Handle changes to non-versioned fields
                //
                if (updatedFieldsByScope.get(Scope.NON_VERSIONED).size() > 0) {
                    if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.NON_VERSIONED))) {
                        indexer.setIndexAllVTags(vtagsToIndex, vtags, indexCase, record);
                        // After this we go to the treatment of changed vtag fields
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s: non-versioned fields changed, will reindex all vtags.",
                                    record.getId()));
                        }
                    }
                }

                //
                // Handle changes to versioned(-mutable) fields
                //
                // If there were non-versioned fields changed, then we already reindex all versions
                // so this can be skipped.
                //
                // In the case of newly created versions that should be indexed: this will often be
                // accompanied by corresponding changes to vtag fields, which are handled next, and in which case
                // it would work as well if this code would not be here.
                //
                if (vtagsToIndex.isEmpty() && (event.getVersionCreated() != -1 || event.getVersionUpdated() != -1)) {
                    if (atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED))
                            || atLeastOneUsedInIndex(updatedFieldsByScope.get(Scope.VERSIONED_MUTABLE))) {

                        long version = event.getVersionCreated() != -1 ? event.getVersionCreated() : event.getVersionUpdated();
                        if (vtagsByVersion.containsKey(version)) {
                            Set<String> tmp = new HashSet<String>();
                            tmp.addAll(indexCase.getVersionTags());
                            tmp.retainAll(vtagsByVersion.get(version));
                            vtagsToIndex.addAll(tmp);

                            if (log.isDebugEnabled()) {
                                log.debug(String.format("Record %1$s: versioned(-mutable) fields changed, will " +
                                        "index for all tags of modified version %2$s that require indexing: %3$s",
                                        record.getId(), version, indexer.vtagSetToNameString(vtagsToIndex)));
                            }
                        }
                    }
                }

                //
                // Handle changes to vtag fields themselves
                //
                Set<String> changedVTagFields = VersionTag.filterVTagFields(event.getUpdatedFields(), typeManager);
                // Remove the vtags which are going to be reindexed anyway
                changedVTagFields.removeAll(vtagsToIndex);
                for (String vtag : changedVTagFields) {
                    if (vtags.containsKey(vtag) && indexCase.getVersionTags().contains(vtag)) {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s: will index for created or updated vtag %2$s",
                                    record.getId(), indexer.safeLoadTagName(vtag)));
                        }
                        vtagsToIndex.add(vtag);
                    } else {
                        // The vtag does not exist anymore on the document, or does not need to be indexed: delete from index
                        indexer.delete(record.getId(), vtag);
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Record %1$s: deleted from index for deleted vtag %2$s",
                                    record.getId(), indexer.safeLoadTagName(vtag)));
                        }
                    }
                }
            }


            //
            // Index
            //
            indexer.index(record, vtagsToIndex, vtags);
        }
    }

    private void updateDenormalizedData(RecordId recordId, RecordEvent event,
            Map<Scope, Set<FieldType>> updatedFieldsByScope, Map<Long, Set<String>> vtagsByVersion) {

        // This algorithm is designed to first collect all the reindex-work, and then to perform it.
        // Otherwise the same document would be indexed multiple times if it would become invalid
        // because of different reasons (= different indexFields).

        //
        // Collect all the relevant IndexFields, and for each the relevant vtags
        //

        // This map will contain all the IndexFields we need to treat, and for each one the vtags to be considered
        Map<IndexField, Set<String>> indexFieldsVTags = new IdentityHashMap<IndexField, Set<String>>() {
            @Override
            public Set<String> get(Object key) {
                if (!this.containsKey(key) && key instanceof IndexField) {
                    this.put((IndexField)key, new HashSet<String>());
                }
                return super.get(key);
            }
        };

        // There are two cases when denormalized data needs updating:
        //   1. when the content of a (vtagged) record changes
        //   2. when vtags change (are added, removed or point to a different version)
        // We now handle these 2 cases.

        // === Case 1 === updates in response to changes to this record

        long version = event.getVersionCreated() == -1 ? event.getVersionUpdated() : event.getVersionCreated();

        // Determine the relevant index fields
        List<IndexField> indexFields;
        if (event.getType() == RecordEvent.Type.DELETE) {
            indexFields = indexer.getConf().getDerefIndexFields();
        } else {
            indexFields = new ArrayList<IndexField>();

            collectDerefIndexFields(updatedFieldsByScope.get(Scope.NON_VERSIONED), indexFields);

            if (version != -1 && vtagsByVersion.get(version) != null) {
                collectDerefIndexFields(updatedFieldsByScope.get(Scope.VERSIONED), indexFields);
                collectDerefIndexFields(updatedFieldsByScope.get(Scope.VERSIONED_MUTABLE), indexFields);
            }
        }

        // For each indexField, determine the vtags of the referrer that we should consider.
        // In the context of this algorithm, a referrer is each record whose index might contain
        // denormalized data from the record of which we are now processing the change event.
        nextIndexField:
        for (IndexField indexField : indexFields) {
            DerefValue derefValue = (DerefValue)indexField.getValue();
            FieldType fieldType = derefValue.getTargetField();

            //
            // Determine the vtags of the referrer that we should consider
            //
            Set<String> referrerVtags = indexFieldsVTags.get(indexField);

            // we do not know if the referrer has any versions at all, so always add the versionless tag
            referrerVtags.add(VersionTag.VERSIONLESS_TAG);

            if (fieldType.getScope() == Scope.NON_VERSIONED || event.getType() == RecordEvent.Type.DELETE) {
                // If it is a non-versioned field, then all vtags should be considered.
                // If it is a delete event, we do not know what vtags existed for the record, so consider them all.
                referrerVtags.addAll(indexer.getConf().getVtags());
            } else {
                // Otherwise only the vtags of the created/updated version, if any
                if (version != -1) {
                    Set<String> vtags = vtagsByVersion.get(version);
                    if (vtags != null)
                        referrerVtags.addAll(vtags);
                }
            }
        }


        // === Case 2 === handle updated/added/removed vtags

        Set<String> changedVTagFields = VersionTag.filterVTagFields(event.getUpdatedFields(), typeManager);
        if (!changedVTagFields.isEmpty()) {
            // In this case, the IndexFields which we need to handle are those that use fields from:
            //  - the previous version to which the vtag pointed (if it is not a new vtag)
            //  - the new version to which the vtag points (if it is not a deleted vtag)
            // But rather than calculating all that (consider the need to retrieve the versions),
            // for now we simply consider all IndexFields.
            // TODO could optimize this to exclude deref fields that use only non-versioned fields?
            for (IndexField indexField : indexer.getConf().getDerefIndexFields()) {
                indexFieldsVTags.get(indexField).addAll(changedVTagFields);
            }
        }

        //
        // Now search the referrers, that is: for each link field, find out which records point to the current record
        // in a certain versioned view (= a certain vtag)
        //

        // This map holds the referrer records to reindex, and for which versions (vtags) they need to be reindexed.
        Map<RecordId, Set<String>> referrersVTags = new HashMap<RecordId, Set<String>>() {
            @Override
            public Set<String> get(Object key) {
                if (!containsKey(key) && key instanceof RecordId) {
                    put((RecordId)key, new HashSet<String>());
                }
                return super.get(key);
            }
        };

        int searchedFollowCount = 0;

        // Run over the IndexFields
        nextIndexField:
        for (Map.Entry<IndexField, Set<String>> entry : indexFieldsVTags.entrySet()) {
            IndexField indexField = entry.getKey();
            Set<String> referrerVTags = entry.getValue();
            DerefValue derefValue = (DerefValue)indexField.getValue();

            // Run over the version tags
            for (String referrerVtag : referrerVTags) {
                List<DerefValue.Follow> follows = derefValue.getFollows();

                Set<RecordId> referrers = new HashSet<RecordId>();
                referrers.add(recordId);

                for (int i = follows.size() - 1; i >= 0; i--) {
                    searchedFollowCount++;
                    DerefValue.Follow follow = follows.get(i);

                    Set<RecordId> newReferrers = new HashSet<RecordId>();

                    if (follow instanceof DerefValue.FieldFollow) {
                        String fieldId = ((DerefValue.FieldFollow)follow).getFieldId();
                        for (RecordId referrer : referrers) {
                            try {
                                Set<RecordId> linkReferrers = linkIndex.getReferrers(referrer, referrerVtag, fieldId);
                                newReferrers.addAll(linkReferrers);
                            } catch (IOException e) {
                                // TODO
                                e.printStackTrace();
                            }
                        }
                    } else if (follow instanceof DerefValue.VariantFollow) {
                        DerefValue.VariantFollow varFollow = (DerefValue.VariantFollow)follow;
                        Set<String> dimensions = varFollow.getDimensions();

                        // We need to find out the variants of the current set of referrers which have the
                        // same variant properties as the referrer (= same key/value pairs) and additionally
                        // have the extra dimensions defined in the VariantFollow.

                        nextReferrer:
                        for (RecordId referrer : referrers) {

                            Map<String, String> refprops = referrer.getVariantProperties();

                            // If the referrer already has one of the dimensions, then skip it
                            for (String dimension : dimensions) {
                                if (refprops.containsKey(dimension))
                                    continue nextReferrer;
                            }

                            //
                            Set<RecordId> variants;
                            try {
                                variants = repository.getVariants(referrer);
                            } catch (RepositoryException e) {
                                // TODO we should probably throw this higher up and let it be handled there
                                throw new RuntimeException(e);
                            }

                            nextVariant:
                            for (RecordId variant : variants) {
                                Map<String, String> varprops = variant.getVariantProperties();

                                // Check it has each of the variant properties of the current referrer record
                                for (Map.Entry<String, String> refprop : refprops.entrySet()) {
                                    if (!ObjectUtils.safeEquals(varprops.get(refprop.getKey()), refprop.getValue())) {
                                        // skip this variant
                                        continue nextVariant;
                                    }
                                }

                                // Check it has the additional dimensions
                                for (String dimension : dimensions) {
                                    if (!varprops.containsKey(dimension))
                                        continue nextVariant;
                                }

                                // We have a hit
                                newReferrers.add(variant);
                            }
                        }
                    } else if (follow instanceof DerefValue.MasterFollow) {
                        for (RecordId referrer : referrers) {
                            // A MasterFollow can only point to masters
                            if (referrer.isMaster()) {
                                Set<RecordId> variants;
                                try {
                                    variants = repository.getVariants(referrer);
                                } catch (RepositoryException e) {
                                    // TODO we should probably throw this higher up and let it be handled there
                                    throw new RuntimeException(e);
                                }

                                variants.remove(referrer);
                                newReferrers.addAll(variants);
                            }
                        }
                    } else {
                        throw new RuntimeException("Unexpected implementation of DerefValue.Follow: " +
                                follow.getClass().getName());
                    }

                    referrers = newReferrers;
                }

                for (RecordId referrer : referrers) {
                    referrersVTags.get(referrer).add(referrerVtag);
                }
            }
        }


        if (log.isDebugEnabled()) {
            log.debug(String.format("Record %1$s: found %2$s records (times vtags) to be updated because they " +
                    "might contain outdated denormalized data. Checked %3$s follow instances.", recordId, referrersVTags.size(), searchedFollowCount));
        }


        //
        // Now re-index all the found referrers
        //
        nextReferrer:
        for (Map.Entry<RecordId, Set<String>> entry : referrersVTags.entrySet()) {
            RecordId referrer = entry.getKey();
            Set<String> vtagsToIndex = entry.getValue();

            boolean lockObtained = false;
            try {
                indexLocker.lock(referrer);
                lockObtained = true;

                IdRecord record = null;
                try {
                    // TODO optimize this: we are only interested to know the vtags and to know if the record has versions
                    record = repository.readWithIds(referrer, null, null);
                } catch (Exception e) {
                    // TODO handle this
                    // One case to be expected here is that the record has been deleted since we read the list of referrers
                    e.printStackTrace();
                }

                IndexCase indexCase = indexer.getConf().getIndexCase(record.getRecordTypeName(), record.getId().getVariantProperties());
                if (indexCase == null) {
                    continue nextReferrer;
                }

                try {
                    if (record.getVersion() == null) {
                        if (indexCase.getIndexVersionless() && vtagsToIndex.contains(VersionTag.VERSIONLESS_TAG)) {
                            indexer.index(record, Collections.singleton(VersionTag.VERSIONLESS_TAG));
                        }
                    } else {
                        Map<String, Long> recordVTags = VersionTag.getTagsById(record, typeManager);
                        vtagsToIndex.retainAll(indexCase.getVersionTags());
                        // Only keep vtags which exist on the record
                        vtagsToIndex.retainAll(recordVTags.keySet());
                        indexer.indexRecord(record.getId(), vtagsToIndex, recordVTags);
                    }
                } catch (Exception e) {
                    // TODO handle this
                    e.printStackTrace();
                }
            } catch (IndexLockException e) {
                // TODO handle this
                e.printStackTrace();
            } finally {
                if (lockObtained) {
                    indexLocker.unlockLogFailure(referrer);
                }
            }
        }


    }

    private void collectDerefIndexFields(Set<FieldType> fieldTypes, List<IndexField> indexFields) {
        for (FieldType fieldType : fieldTypes) {
            indexFields.addAll(indexer.getConf().getDerefIndexFields(fieldType.getId()));
        }
    }

    private boolean atLeastOneUsedInIndex(Set<FieldType> fieldTypes) {
        for (FieldType type : fieldTypes) {
            if (indexer.getConf().isIndexFieldDependency(type.getId())) {
                return true;
            }
        }
        return false;
    }

    private Map<Scope, Set<FieldType>> getFieldTypeAndScope(Set<String> fieldIds) {
        Map<Scope, Set<FieldType>> result = new HashMap<Scope, Set<FieldType>>();
        for (Scope scope : Scope.values()) {
            result.put(scope, new HashSet<FieldType>());
        }

        for (String fieldId : fieldIds) {
            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeById(fieldId);
            } catch (FieldTypeNotFoundException e) {
                continue;
            } catch (RepositoryException e) {
                // TODO not sure what to do in these kinds of situations
                throw new RuntimeException(e);
            }
            result.get(fieldType.getScope()).add(fieldType);
        }

        return result;
    }

    private class IndexUpdaterMetrics implements Updater {
        private long msgProcessingTime = 0;
        private int msgCount = 0;
        private MetricsRecord record;

        public IndexUpdaterMetrics() {
            MetricsContext lilyContext = MetricsUtil.getContext("lily");
            record = lilyContext.createRecord("indexUpdater");
            lilyContext.registerUpdater(this);
        }

        public synchronized void doUpdates(MetricsContext unused) {
            record.setMetric("averageTime", msgCount == 0 ? 0 : (msgProcessingTime / msgCount));
            record.incrMetric("processedMsgCount", msgCount);
            record.update();

            msgProcessingTime = 0;
            msgCount = 0;
        }

        synchronized void messageProcessed(long time) {
            msgProcessingTime += time;
            msgCount++;
        }
    }

}

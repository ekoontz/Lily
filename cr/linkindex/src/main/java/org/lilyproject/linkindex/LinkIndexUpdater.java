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
package org.lilyproject.linkindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.api.RecordNotFoundException;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;

import java.util.*;

import static org.lilyproject.util.repo.RecordEvent.Type.*;

// TODO think more about error processing:
//      Some kinds of errors might be temporary in nature and be solved by retrying after some time.
//      This would seem preferable rather than just giving up and leaving the link index in an incorrect state.
//      Also to consider: if an error occurs, then delete all links for the record and or vtag, rather than just
//      logging the error

/**
 * Keeps the {@link LinkIndex} up to date when changes happen to records.
 */
public class LinkIndexUpdater implements RowLogMessageListener {
    private Repository repository;
    private TypeManager typeManager;
    private LinkIndex linkIndex;

    private Log log = LogFactory.getLog(getClass());

    public LinkIndexUpdater(Repository repository, LinkIndex linkIndex) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.linkIndex = linkIndex;
    }

    public boolean processMessage(RowLogMessage msg) {
        try {
            RecordId recordId = repository.getIdGenerator().fromBytes(msg.getRowKey());
            RecordEvent recordEvent = new RecordEvent(msg.getPayload());
            update(recordId, recordEvent);
        } catch (Exception e) {
            log.error("Error processing event in LinkIndexUpdater", e);
        }
        return true;
    }

    public void update(RecordId recordId, RecordEvent recordEvent) {
        // This is the algorithm for updating the LinkIndex when a record changes.
        //
        // The LinkIndex contains:
        //  * for records that have versions: for each vtag, the extracted links from the record in that
        //    version (includes all scopes). If the record has no vtags, there will hence be no entries in
        //    the link index
        //  * for records without versions: the links extracted from the non-versioned content are stored
        //    under the special vtag @@versionless
        //
        // There are basically two kinds of changes that require updating the link index:
        //  * the content of (non-vtag) fields is changed
        //  * the vtags change: existing vtag now points to another version, a new vtag is added, or a vtag is removed
        //

        try {
            if (recordEvent.getType().equals(DELETE)) {
                // Delete everything from the link index for this record, thus for all vtags
                linkIndex.deleteLinks(recordId);
                if (log.isDebugEnabled()) {
                    log.debug("Record " + recordId + " : delete event : deleted extracted links.");
                }
            } else if (recordEvent.getType().equals(CREATE) || recordEvent.getType().equals(UPDATE)) {

                // If the record is not new but its first version was created now, there might be existing
                // entries for the @@versionless vtag
                if (recordEvent.getType() == RecordEvent.Type.UPDATE && recordEvent.getVersionCreated() == 1) {
                    linkIndex.deleteLinks(recordId, VersionTag.VERSIONLESS_TAG);
                }

                IdRecord record;
                try {
                    record = repository.readWithIds(recordId, null, null);
                } catch (RecordNotFoundException e) {
                    // record not found: delete all links for all vtags
                    linkIndex.deleteLinks(recordId);
                    if (log.isDebugEnabled()) {
                        log.debug("Record " + recordId + " : does not exist : deleted extracted links.");
                    }
                    return;
                }
                boolean hasVersions = record.getVersion() != null;

                if (hasVersions) {
                    Map<String, Long> vtags = VersionTag.getTagsById(record, typeManager);
                    Map<Long, Set<String>> tagsByVersion = VersionTag.tagsByVersion(vtags);

                    //
                    // First find out for what vtags we need to re-perform the link extraction
                    //
                    Set<String> vtagsToProcess = new HashSet<String>();

                    // Modified vtag fields
                    Set<String> changedVTags = VersionTag.filterVTagFields(recordEvent.getUpdatedFields(), typeManager);
                    vtagsToProcess.addAll(changedVTags);

                    // The vtags of the created/modified version, if any
                    Set<String> vtagsOfChangedVersion = null;
                    if (recordEvent.getVersionCreated() != -1) {
                        vtagsOfChangedVersion = tagsByVersion.get(recordEvent.getVersionCreated());
                    } else if (recordEvent.getVersionUpdated() != -1) {
                        vtagsOfChangedVersion = tagsByVersion.get(recordEvent.getVersionUpdated());
                    }

                    if (vtagsOfChangedVersion != null) {
                        vtagsToProcess.addAll(vtagsOfChangedVersion);
                    }

                    //
                    // For each of the vtags, perform the link extraction
                    //
                    Map<Long, Set<FieldedLink>> cache = new HashMap<Long, Set<FieldedLink>>();
                    for (String vtag : vtagsToProcess) {
                        if (!vtags.containsKey(vtag)) {
                            // The vtag is not defined on the document: it is a deleted vtag, delete the
                            // links corresponding to it
                            linkIndex.deleteLinks(recordId, vtag);
                            if (log.isDebugEnabled()) {
                                log.debug(String.format("Record %1$s, vtag %2$s : deleted extracted links",
                                        record.getId(), safeLoadTagName(vtag)));
                            }
                        } else {
                            // Since one version might have multiple vtags, we keep a little cache to avoid
                            // extracting the links from the same version twice.
                            long version = vtags.get(vtag);
                            Set<FieldedLink> links;
                            if (cache.containsKey(version)) {
                                links = cache.get(version);
                            } else {
                                links = extractLinks(recordId, version);
                                cache.put(version, links);
                            }
                            linkIndex.updateLinks(recordId, vtag, links);
                            if (log.isDebugEnabled()) {
                                log.debug(String.format("Record %1$s, vtag %2$s : extracted links count : %3$s",
                                        record.getId(), safeLoadTagName(vtag), links.size()));
                            }
                        }
                    }
                } else {
                    // The record has no versions
                    Set<FieldedLink> links = extractLinks(recordId, null);
                    linkIndex.updateLinks(recordId, VersionTag.VERSIONLESS_TAG, links);
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Record %1$s, vtag %2$s : extracted links count : %3$s",
                                record.getId(), VersionTag.VERSIONLESS_TAG, links.size()));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error processing event in LinkIndexUpdater", e);
        }
    }

    private Set<FieldedLink> extractLinks(RecordId recordId, Long version) {
        try {
            Set<FieldedLink> links;
            IdRecord versionRecord = null;
            try {
                versionRecord = repository.readWithIds(recordId, version, null);
            } catch (RecordNotFoundException e) {
                // vtag points to a non-existing record
            }

            if (versionRecord == null) {
                links = Collections.emptySet();
            } else {
                LinkCollector collector = new LinkCollector();
                RecordLinkExtractor.extract(versionRecord, collector, repository);
                links = collector.getLinks();
            }
            return links;
        } catch (VersionNotFoundException e) {
            // A vtag pointing to a non-existing version, nothing unusual.
            return Collections.emptySet();
        } catch (Throwable t) {
            log.error("Error extracting links from record " + recordId, t);
        }
        return Collections.emptySet();
    }

    /**
     * Lookup name of field type, for use in debug logs. Beware, this might be slow.
     */
    private String safeLoadTagName(String fieldTypeId) {
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

}

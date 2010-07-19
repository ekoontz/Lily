package org.lilycms.linkindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.util.repo.RecordEvent;
import org.lilycms.util.repo.VersionTag;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;

import java.util.*;

import static org.lilycms.util.repo.RecordEvent.Type.*;

// TODO think more about error processing:
//      Some kinds of errors might be temporary in nature and be solved by retrying after some time.
//      This would seem preferable rather than just giving up and leaving the link index in an incorrect state.
//      Also to consider: if an error occurs, then delete all links for the record and or vtag, rather than just
//      logging the error

/**
 * Keeps the {@link LinkIndex} up to date when changes happen to records.
 */
public class LinkIndexUpdater {
    private Repository repository;
    private TypeManager typeManager;
    private RowLog rowLog;
    private LinkIndex linkIndex;
    private MyListener listener = new MyListener();

    private Log log = LogFactory.getLog(getClass());

    public LinkIndexUpdater(Repository repository, LinkIndex linkIndex) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.linkIndex = linkIndex;
    }

    /**
     * This constructor registers a listener with the rowlog.
     *
     * <b>IMPORTANT: this should not be used, the link index update is now triggered as part
     * of the indexer.</b>
     */
    public LinkIndexUpdater(Repository repository, LinkIndex linkIndex, RowLog rowLog) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.linkIndex = linkIndex;

        if (rowLog != null) {
            this.rowLog = rowLog;
            rowLog.registerConsumer(listener);
        }
    }

    public void stop() {
        rowLog.unRegisterConsumer(listener);
    }

    private class  MyListener implements RowLogMessageConsumer {

        public int getId() {
            return 102;
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

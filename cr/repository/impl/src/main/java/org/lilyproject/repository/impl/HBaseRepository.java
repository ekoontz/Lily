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
package org.lilyproject.repository.impl;

import static org.lilyproject.util.hbase.LilyHBaseSchema.DELETE_MARKER;
import static org.lilyproject.util.hbase.LilyHBaseSchema.EXISTS_FLAG;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.RepositoryMetrics.Action;
import org.lilyproject.repository.impl.RepositoryMetrics.HBaseAction;
import org.lilyproject.repository.impl.lock.RowLocker;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Pair;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordCf;
import org.lilyproject.util.hbase.LilyHBaseSchema.RecordColumn;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.repo.RecordEvent.Type;

/**
 * Repository implementation.
 * 
 */
public class HBaseRepository implements Repository {
 

    private HTableInterface recordTable;
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private byte[] columnFamily = RecordCf.DATA.bytes;
    private byte[] systemColumnFamily = RecordCf.SYSTEM.bytes;
    private Map<Scope, byte[]> recordTypeIdColumnNames = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeVersionColumnNames = new HashMap<Scope, byte[]>();
    private BlobStoreAccessRegistry blobStoreAccessRegistry;
    private RowLog wal;
    private RowLocker rowLocker;
    
    private Log log = LogFactory.getLog(getClass());
    private RepositoryMetrics metrics;
    private final HBaseTableFactory hbaseTableFactory;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator,
            BlobStoreAccessFactory blobStoreAccessFactory, RowLog wal, Configuration configuration, HBaseTableFactory hbaseTableFactory) throws IOException {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.wal = wal;
        this.hbaseTableFactory = hbaseTableFactory;
        blobStoreAccessRegistry = new BlobStoreAccessRegistry();
        blobStoreAccessRegistry.setBlobStoreAccessFactory(blobStoreAccessFactory);
        recordTable = hbaseTableFactory.getRecordTable();

        recordTypeIdColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_ID.bytes);
        recordTypeIdColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONED, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED, RecordColumn.VERSIONED_RT_VERSION.bytes);
        recordTypeVersionColumnNames.put(Scope.VERSIONED_MUTABLE, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);

        rowLocker = new RowLocker(recordTable, RecordCf.SYSTEM.bytes, RecordColumn.LOCK.bytes, 10000);
        metrics = new RepositoryMetrics("hbaserepository");
    }

    public void close() throws IOException {
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }

    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }

    public Record createOrUpdate(Record record) throws FieldTypeNotFoundException, RecordException,
            RecordTypeNotFoundException, InvalidRecordException, TypeException,
            VersionNotFoundException, RecordLockedException {
        return createOrUpdate(record, true);
    }

    public Record createOrUpdate(Record record, boolean useLatestRecordType) throws FieldTypeNotFoundException,
            RecordException, RecordTypeNotFoundException, InvalidRecordException, TypeException,
            VersionNotFoundException, RecordLockedException {

        if (record.getId() == null) {
            // While we could generate an ID ourselves in this case, this would defeat partly the purpose of
            // createOrUpdate, which is that clients would be able to retry the operation (in case of IO exceptions)
            // without having to worry that more than one record might be created.
            throw new RecordException("Record ID is mandatory when using create-or-update.");
        }

        byte[] rowId = record.getId().toBytes();
        Get get = new Get(rowId);
        get.addColumn(RecordCf.SYSTEM.bytes, RecordColumn.DELETED.bytes);

        int attempts;
        
        for (attempts = 0; attempts < 3; attempts++) {            
            Result result;
            try {
                result = recordTable.get(get);
            } catch (IOException e) {
                throw new RecordException("Error reading record row for record id " + record.getId());
            }

            byte[] deleted = result.getValue(systemColumnFamily, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                // do the create
                try {
                    Record createdRecord = create(record);
                    return createdRecord;
                } catch (RecordExistsException e) {
                     // someone created the record since we checked, we will try again
                }
            } else {
                // do the update
                try {
                    record = update(record, false, useLatestRecordType);
                    return record;
                } catch (RecordNotFoundException e) {
                    // some deleted the record since we checked, we will try again
                }
            }
        }

        throw new RecordException("Create-or-update failed after " + attempts +
                " attempts, toggling between create and update mode.");
    }

    public Record create(Record record) throws RecordExistsException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException,
            RecordLockedException {

        long before = System.currentTimeMillis();
        try {
            checkCreatePreconditions(record);
            
            Record newRecord = record.clone();
    
            RecordId recordId = newRecord.getId();
            if (recordId == null) {
                recordId = idGenerator.newRecordId();
                newRecord.setId(recordId);
            }
    
            byte[] rowId = recordId.toBytes();
            RowLock rowLock = null;
            org.lilyproject.repository.impl.lock.RowLock customRowLock = null;
            RowLogMessage walMessage;
    
            try {
    
                // Take HBase RowLock
                rowLock = recordTable.lockRow(rowId);
                if (rowLock == null)
                    throw new RecordException("Failed to lock row while creating record <" + recordId
                            + "> in HBase table", null);
    
                long version = 1L;
                // If the record existed it would have been deleted.
                // The version numbering continues from where it has been deleted.
                Get get = new Get(rowId, rowLock);
                get.addColumn(systemColumnFamily, RecordColumn.VERSION.bytes);
                Result result = recordTable.get(get);
                if (!result.isEmpty()) {
	                byte[] oldVersion = result.getValue(systemColumnFamily, RecordColumn.VERSION.bytes);
	                if (oldVersion != null) {
	                    version = Bytes.toLong(oldVersion);
	                }
                }
                
                Record dummyOriginalRecord = newRecord();
                Put put = new Put(newRecord.getId().toBytes(), rowLock);
                put.add(systemColumnFamily, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(false));
                RecordEvent recordEvent = new RecordEvent();
                recordEvent.setType(Type.CREATE);
                calculateRecordChanges(newRecord, dummyOriginalRecord, version, put, recordEvent, false);
                // Make sure the record type changed flag stays false for a newly
                // created record
                recordEvent.setRecordTypeChanged(false);
                if (newRecord.getVersion() != null)
                    recordEvent.setVersionCreated(newRecord.getVersion());
    
                walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
                long beforeHbase = System.currentTimeMillis();
                recordTable.put(put);
                metrics.reportHBase(HBaseAction.PUT, System.currentTimeMillis()-beforeHbase);
                
                // Take Custom RowLock before releasing the HBase RowLock
                try {
                    customRowLock = lockRow(recordId, rowLock);
                } catch (IOException ignore) {
                    log.info("Exception while taking a custom rowLock. Processing the wal message will be retried later.", ignore);
                }
            } catch (IOException e) {
                throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                        e);
            } catch (RowLogException e) {
                throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                        e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                        e);
            } finally {
                if (rowLock != null) {
                    try {
                        recordTable.unlockRow(rowLock);
                    } catch (IOException e) {
                        log.info("Exception while unlocking HBase lock <"+rowLock+">", e);
                    }
                }
            }
    
            if (customRowLock != null) {
                try {
                    wal.processMessage(walMessage);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Processing message <"+walMessage+"> by the WAL got interrupted. It will be retried later.", e);
                } catch (RowLogException e) {
                    log.warn("Exception while processing message <"+walMessage+"> by the WAL. It will be retried later.", e);
                } finally {
                    unlockRow(customRowLock);
                }
            }

            newRecord.setResponseStatus(ResponseStatus.CREATED);
            return newRecord;
        } finally {
            metrics.report(Action.CREATE, System.currentTimeMillis() - before);
        }
    }

    private void checkCreatePreconditions(Record record) throws InvalidRecordException {
        ArgumentValidator.notNull(record, "record");
        if (record.getRecordTypeName() == null) {
            throw new InvalidRecordException(record, "The recordType cannot be null for a record to be created.");
        }
        if (record.getFields().isEmpty()) {
            throw new InvalidRecordException(record, "Creating an empty record is not allowed");
        }
    }

    public Record update(Record record, boolean updateVersion, boolean useLatestRecordType)
            throws InvalidRecordException, RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException,
            RecordLockedException {

        long before = System.currentTimeMillis();
        try {
            if (record.getId() == null) {
                throw new InvalidRecordException(record, "The recordId cannot be null for a record to be updated.");
            }
            try {
                if (!checkAndProcessOpenMessages(record.getId())) {
                    throw new RecordException("Record <"+ record.getId()+"> update could not be performed due to remaining messages on the WAL");
                }
            }  catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RecordException("Exception occurred while updating record <" + record.getId() + ">",
                        e);
            }
            
            if (updateVersion) {
                return updateMutableFields(record, useLatestRecordType);
            } else {
                return updateRecord(record, useLatestRecordType);
            }
        } finally {
            metrics.report(Action.UPDATE, System.currentTimeMillis() - before);
        }
    }
    
    public Record update(Record record) throws InvalidRecordException, RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, RecordLockedException {
        return update(record, false, true);
    }
    
    private Record updateRecord(Record record, boolean useLatestRecordType) throws RecordNotFoundException,
            InvalidRecordException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException, RecordLockedException {
        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        org.lilyproject.repository.impl.lock.RowLock rowLock = null;
        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);

            Record originalRecord = read(newRecord.getId(), null, null, new ReadContext());

            Put put = new Put(newRecord.getId().toBytes());
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            long newVersion = originalRecord.getVersion() == null ? 1 : originalRecord.getVersion() + 1;
            if (calculateRecordChanges(newRecord, originalRecord, newVersion, put, recordEvent, useLatestRecordType)) {
                putMessageOnWalAndProcess(recordId, rowLock, put, recordEvent);
                newRecord.setResponseStatus(ResponseStatus.UPDATED);
            } else {
                newRecord.setResponseStatus(ResponseStatus.UP_TO_DATE);
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while putting updated record <" + recordId
                    + "> on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } finally {
            unlockRow(rowLock);
        }
        return newRecord;
    }

    private void putMessageOnWalAndProcess(RecordId recordId, org.lilyproject.repository.impl.lock.RowLock rowLock,
            Put put, RecordEvent recordEvent) throws InterruptedException, RowLogException, IOException, RecordException {
        RowLogMessage walMessage;
        walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
        if (!rowLocker.put(put, rowLock)) {
            throw new RecordException("Exception occurred while putting updated record <" + recordId
                    + "> on HBase table", null);
        }

        if (walMessage != null) {
            try {
                wal.processMessage(walMessage);
            } catch (RowLogException e) {
                // Processing the message failed, it will be retried later.
            }
        }
    }

    // Calculates the changes that are to be made on the record-row and puts
    // this information on the Put object and the RecordEvent
    private boolean calculateRecordChanges(Record record, Record originalRecord, Long version, Put put,
            RecordEvent recordEvent, boolean useLatestRecordType) throws RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, TypeException, InvalidRecordException, InterruptedException {
        QName recordTypeName = record.getRecordTypeName();
        Long recordTypeVersion = null;
        if (recordTypeName == null) {
            recordTypeName = originalRecord.getRecordTypeName();
        } else {
            recordTypeVersion = useLatestRecordType ? null : record.getRecordTypeVersion();
        }

        RecordType recordType = typeManager.getRecordTypeByName(recordTypeName, recordTypeVersion);
        
        // Check which fields have changed
        Set<Scope> changedScopes = calculateChangedFields(record, originalRecord, recordType, version, put, recordEvent);

        // If no versioned fields have changed, keep the original version
        boolean versionedFieldsHaveChanged = changedScopes.contains(Scope.VERSIONED)
                || changedScopes.contains(Scope.VERSIONED_MUTABLE);
        if (!versionedFieldsHaveChanged) {
            version = originalRecord.getVersion();
        }

        boolean fieldsHaveChanged = !changedScopes.isEmpty();
        if (fieldsHaveChanged) {
         // The provided recordTypeVersion could have been null, so the latest version of the recordType was taken and we need to know which version that is
            recordTypeVersion = recordType.getVersion(); 
            if (!recordTypeName.equals(originalRecord.getRecordTypeName())
                    || !recordTypeVersion.equals(originalRecord.getRecordTypeVersion())) {
                recordEvent.setRecordTypeChanged(true);
                put.add(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_ID.bytes, 1L, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_VERSION.bytes, 1L,
                        Bytes.toBytes(recordTypeVersion));
            }
            // Always set the record type on the record since the requested
            // record type could have been given without a version number
            record.setRecordType(recordTypeName, recordTypeVersion);
            if (version != null) {
                byte[] versionBytes = Bytes.toBytes(version);
                put.add(systemColumnFamily, RecordColumn.VERSION.bytes, 1L, versionBytes);
                if (VersionTag.hasLastVTag(recordType, typeManager) || VersionTag.hasLastVTag(record, typeManager) || VersionTag.hasLastVTag(originalRecord, typeManager)) {
                    FieldTypeImpl lastVTagType = (FieldTypeImpl)VersionTag.getLastVTagType(typeManager);
                    put.add(columnFamily, lastVTagType.getIdBytes(), 1L, encodeFieldValue(lastVTagType, version));
                    record.setField(lastVTagType.getName(), version);
                }
            }
            validateRecord(record, originalRecord, recordType);

        }
        // Always set the version on the record. If no fields were changed this
        // will give the latest version in the repository
        record.setVersion(version);

        if (versionedFieldsHaveChanged) {
            recordEvent.setVersionCreated(version);
        }
        
        // Clear the list of deleted fields, as this is typically what the user will expect when using the
        // record object for future updates. 
        record.getFieldsToDelete().clear();
        return fieldsHaveChanged;
    }
    
    private void validateRecord(Record record, Record originalRecord, RecordType recordType) throws
            FieldTypeNotFoundException, TypeException, InvalidRecordException, InterruptedException {
        // Check mandatory fields
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        List<QName> fieldsToDelete = record.getFieldsToDelete();
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            if (fieldTypeEntry.isMandatory()) {
                FieldType fieldType = typeManager.getFieldTypeById(fieldTypeEntry.getFieldTypeId());
                QName fieldName = fieldType.getName();
                if (fieldsToDelete.contains(fieldName)) {
                    throw new InvalidRecordException(record, "Field: <"+fieldName+"> is mandatory.");
                }
                try {
                    record.getField(fieldName); 
                } catch (FieldNotFoundException notFoundOnNewRecord) {
                    try {
                        originalRecord.getField(fieldName);
                    } catch (FieldNotFoundException notFoundOnOriginalRecord) {
                        throw new InvalidRecordException(record, "Field: <"+fieldName+"> is mandatory.");
                    }
                }
            }
        }
    }

    private Set<Scope> calculateChangedFields(Record record, Record originalRecord, RecordType recordType,
            Long version, Put put, RecordEvent recordEvent) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException, InterruptedException {
        Map<QName, Object> originalFields = originalRecord.getFields();
        Set<Scope> changedScopes = new HashSet<Scope>();
        
        Map<QName, Object> fields = getFieldsToUpdate(record);
        
        changedScopes.addAll(calculateUpdateFields(fields, originalFields, null, version, put, recordEvent));
        
        // Update record types
        for (Scope scope : changedScopes) {
            if (Scope.NON_VERSIONED.equals(scope)) {
                put.add(systemColumnFamily, recordTypeIdColumnNames.get(scope), 1L, Bytes.toBytes(recordType
                        .getId()));
                put.add(systemColumnFamily, recordTypeVersionColumnNames.get(scope), 1L, Bytes
                        .toBytes(recordType.getVersion()));
            } else {
                put.add(systemColumnFamily, recordTypeIdColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamily, recordTypeVersionColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getVersion()));

            }
            record.setRecordType(scope, recordType.getName(), recordType.getVersion());
        }
        return changedScopes;
    }

    private Map<QName, Object> getFieldsToUpdate(Record record) {
        // Work with a copy of the map
        Map<QName, Object> fields = new HashMap<QName, Object>();
        fields.putAll(record.getFields());
        for (QName qName : record.getFieldsToDelete()) {
            fields.put(qName, DELETE_MARKER);
        }
        return fields;
    }

    private Set<Scope> calculateUpdateFields(Map<QName, Object> fields, Map<QName, Object> originalFields, Map<QName, Object> originalNextFields,
            Long version, Put put, RecordEvent recordEvent) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException, InterruptedException {
        Set<Scope> changedScopes = new HashSet<Scope>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            QName fieldName = field.getKey();
            Object newValue = field.getValue();
            boolean fieldIsNewOrDeleted = !originalFields.containsKey(fieldName);
            Object originalValue = originalFields.get(fieldName);
            if (!(
                    ((newValue == null) && (originalValue == null))         // Don't update if both are null
                    || (isDeleteMarker(newValue) && fieldIsNewOrDeleted)    // Don't delete if it doesn't exist
                    || (newValue.equals(originalValue)))) {                 // Don't update if they didn't change
                FieldTypeImpl fieldType = (FieldTypeImpl)typeManager.getFieldTypeByName(fieldName);
                Scope scope = fieldType.getScope();
                byte[] fieldIdAsBytes = fieldType.getIdBytes();
                byte[] encodedFieldValue = encodeFieldValue(fieldType, newValue);

                if (Scope.NON_VERSIONED.equals(scope)) {
                    put.add(columnFamily, fieldIdAsBytes, 1L, encodedFieldValue);
                } else {
                    put.add(columnFamily, fieldIdAsBytes, version, encodedFieldValue);
                    if (originalNextFields != null && !fieldIsNewOrDeleted && originalNextFields.containsKey(fieldName)) {
                        copyValueToNextVersionIfNeeded(version, put, originalNextFields, fieldName, originalValue);
                    }
                }
                
                changedScopes.add(scope);

                recordEvent.addUpdatedField(fieldType.getId());
            }
        }
        return changedScopes;
    }

    private boolean updateNeeded(Object newValue, boolean fieldIsNewOrDeleted, Object originalValue) {
        return (!(((newValue == null) && (originalValue == null)) || (isDeleteMarker(newValue) && fieldIsNewOrDeleted) || (newValue.equals(originalValue))));
    }
    
    private byte[] encodeFieldValue(FieldType fieldType, Object fieldValue) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException {
        if (isDeleteMarker(fieldValue))
            return DELETE_MARKER;
        ValueType valueType = fieldType.getValueType();

        // TODO validate with Class#isAssignableFrom()
        byte[] encodedFieldValue = valueType.toBytes(fieldValue);
        encodedFieldValue = EncodingUtil.prefixValue(encodedFieldValue, EXISTS_FLAG);
        return encodedFieldValue;
    }

    private boolean isDeleteMarker(Object fieldValue) {
        return (fieldValue instanceof byte[]) && Arrays.equals(DELETE_MARKER, (byte[])fieldValue);
    }

    private Record updateMutableFields(Record record, boolean latestRecordType) throws InvalidRecordException,
            RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            VersionNotFoundException, TypeException, RecordLockedException {

        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        org.lilyproject.repository.impl.lock.RowLock rowLock = null;
        
        Long version = record.getVersion();
        if (version == null) {
            throw new InvalidRecordException(record,
                    "The version of the record cannot be null to update mutable fields");
        }

        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);
            
            Record originalRecord = read(record.getId(), version, null, new ReadContext());

            // Update the mutable fields
            Put put = new Put(record.getId().toBytes());
            Map<QName, Object> fields = getFieldsToUpdate(record);
            fields = filterMutableFields(fields);
            Map<QName, Object> originalFields = filterMutableFields(originalRecord.getFields());
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            recordEvent.setVersionUpdated(version);

            Set<Scope> changedScopes = calculateUpdateFields(fields, originalFields, getOriginalNextFields(recordId, version), version, put, recordEvent);

            if (!changedScopes.isEmpty()) {
                Long recordTypeVersion = latestRecordType ? null : record.getRecordTypeVersion();
                RecordType recordType = typeManager.getRecordTypeByName(record.getRecordTypeName(), recordTypeVersion);
                
                // Update the mutable record type
                Scope scope = Scope.VERSIONED_MUTABLE;
                newRecord.setRecordType(scope, recordType.getName(), recordType.getVersion());
                
                validateRecord(record, originalRecord, recordType);

                put.add(systemColumnFamily, recordTypeIdColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamily, recordTypeVersionColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getVersion()));

                recordEvent.setVersionUpdated(version);

                putMessageOnWalAndProcess(recordId, rowLock, put, recordEvent);
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId+ "> on HBase table", e);
        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } finally {
            unlockRow(rowLock);
        }
        return newRecord;
    }

    private Map<QName, Object> filterMutableFields(Map<QName, Object> fields) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException, InterruptedException {
        Map<QName, Object> mutableFields = new HashMap<QName, Object>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            FieldType fieldType = typeManager.getFieldTypeByName(field.getKey());
            if (Scope.VERSIONED_MUTABLE.equals(fieldType.getScope())) {
                mutableFields.put(field.getKey(), field.getValue());
            }
        }
        return mutableFields;
    }

    /**
     * If the original value is the same as for the next version
     * this means that the cell at the next version does not contain any value yet,
     * and the record relies on what is in the previous cell's version.
     * The original value needs to be copied into it. Otherwise we loose that value.
     */
    private void copyValueToNextVersionIfNeeded(Long version, Put put, Map<QName, Object> originalNextFields,
            QName fieldName, Object originalValue)
            throws FieldTypeNotFoundException, RecordTypeNotFoundException, RecordException, TypeException,
            InterruptedException {
        Object originalNextValue = originalNextFields.get(fieldName);
        if ((originalValue == null && originalNextValue == null) || originalValue.equals(originalNextValue)) {
            FieldTypeImpl fieldType = (FieldTypeImpl)typeManager.getFieldTypeByName(fieldName);
            byte[] encodedValue = encodeFieldValue(fieldType, originalValue);
            put.add(columnFamily, fieldType.getIdBytes(), version + 1, encodedValue);
        }
    }

    private Map<QName, Object> getOriginalNextFields(RecordId recordId, Long version)
            throws RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException,
            TypeException, RecordNotFoundException, InterruptedException {
        try {
            Record originalNextRecord = read(recordId, version + 1, null, new ReadContext());
            return filterMutableFields(originalNextRecord.getFields());
        } catch (VersionNotFoundException exception) {
            // There is no next record
            return null;
        }
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException,
            InterruptedException {
        return read(recordId, null, null);
    }

    public Record read(RecordId recordId, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        return read(recordId, null, fieldNames);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException,
            InterruptedException {
        return read(recordId, version, null);
    }

    public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        List<FieldType> fields = getFieldTypesFromNames(fieldNames);

        return read(recordId, version, fields, new ReadContext());
    }

    private List<FieldType> getFieldTypesFromNames(List<QName> fieldNames) throws FieldTypeNotFoundException,
            TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldNames != null) {
            fields = new ArrayList<FieldType>();
            for (QName fieldName : fieldNames) {
                fields.add(typeManager.getFieldTypeByName(fieldName));
            }
        }
        return fields;
    }
    
    private List<FieldType> getFieldTypesFromIds(List<String> fieldIds) throws FieldTypeNotFoundException,
            TypeException, InterruptedException {
        List<FieldType> fields = null;
        if (fieldIds != null) {
            fields = new ArrayList<FieldType>(fieldIds.size());
            for (String fieldId : fieldIds) {
                fields.add(typeManager.getFieldTypeById(fieldId));
            }
        }
        return fields;
    }
    
    public List<Record> readVersions(RecordId recordId, Long fromVersion, Long toVersion, List<QName> fieldNames)
            throws FieldTypeNotFoundException, TypeException, RecordNotFoundException, RecordException,
            VersionNotFoundException, RecordTypeNotFoundException, InterruptedException {
        ArgumentValidator.notNull(recordId, "recordId");
        ArgumentValidator.notNull(fromVersion, "fromVersion");
        ArgumentValidator.notNull(toVersion, "toVersion");
        if (fromVersion > toVersion) {
            throw new IllegalArgumentException("fromVersion <" + fromVersion + "> must be smaller or equal to toVersion <" + toVersion + ">");
        }

        List<FieldType> fields = getFieldTypesFromNames(fieldNames);

        Result result = getRow(recordId, toVersion, true, fields);
        if (fromVersion < 1L)
            fromVersion = 1L;
        Long latestVersion = getLatestVersion(result);
        if (latestVersion < toVersion)
            toVersion = latestVersion;
        List<Record> records = new ArrayList<Record>();
        for (long version = fromVersion; version <= toVersion; version++) {
            records.add(getRecordFromRowResult(recordId, version, new ReadContext(), result));
        }
        return records;
    }
    
    public IdRecord readWithIds(RecordId recordId, Long version, List<String> fieldIds) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException,
            TypeException, InterruptedException {
        ReadContext readContext = new ReadContext();

        List<FieldType> fields = getFieldTypesFromIds(fieldIds);

        Record record = read(recordId, version, fields, readContext);

        Map<String, QName> idToQNameMapping = new HashMap<String, QName>();
        for (FieldType fieldType : readContext.getFieldTypes().values()) {
            idToQNameMapping.put(fieldType.getId(), fieldType.getName());
        }

        Map<Scope, String> recordTypeIds = new HashMap<Scope, String>();
        for (Map.Entry<Scope, RecordType> entry : readContext.getRecordTypes().entrySet()) {
            recordTypeIds.put(entry.getKey(), entry.getValue().getId());
        }

        return new IdRecordImpl(record, idToQNameMapping, recordTypeIds);
    }
    
    private Record read(RecordId recordId, Long requestedVersion, List<FieldType> fields, ReadContext readContext)
            throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, VersionNotFoundException, TypeException, InterruptedException {
        long before = System.currentTimeMillis();
        try {
            ArgumentValidator.notNull(recordId, "recordId");
    
            Result result = getRow(recordId, requestedVersion, false, fields);
    
            Long latestVersion = getLatestVersion(result);
            if (requestedVersion == null) {
                requestedVersion = latestVersion;
            } else {
                if (latestVersion == null || latestVersion < requestedVersion ) {
                    Record record = newRecord(recordId);
                    record.setVersion(requestedVersion);
                    throw new VersionNotFoundException(record);
                }
            }
            return getRecordFromRowResult(recordId, requestedVersion, readContext, result);
        } finally {
            metrics.report(Action.READ, System.currentTimeMillis() - before);
        }
    }

    private Record getRecordFromRowResult(RecordId recordId, Long requestedVersion, ReadContext readContext,
            Result result) throws VersionNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, TypeException, InterruptedException {
        Record record = newRecord(recordId);
        record.setVersion(requestedVersion);

        // Extract the actual fields from the retrieved data
        if (extractFieldsAndRecordTypes(result, requestedVersion, record, readContext)) {
            // Set the recordType explicitly in case only versioned fields were
            // extracted
            Pair<String, Long> recordTypePair = extractRecordType(Scope.NON_VERSIONED, result, null, record);
            RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
            record.setRecordType(recordType.getName(), recordType.getVersion());
            readContext.setRecordTypeId(Scope.NON_VERSIONED, recordType);
        }
        return record;
    }

    private Long getLatestVersion(Result result) {
        byte[] latestVersionBytes = result.getValue(systemColumnFamily, RecordColumn.VERSION.bytes);
        Long latestVersion = latestVersionBytes != null ? Bytes.toLong(latestVersionBytes) : null;
        return latestVersion;
    }

    // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    private Result getRow(RecordId recordId, Long version, boolean multipleVersions, List<FieldType> fields)
            throws RecordNotFoundException, RecordException {
        Result result;
        Get get = new Get(recordId.toBytes());
        get.addFamily(columnFamily);
        get.addFamily(systemColumnFamily);
        
        try {
            // Add the columns for the fields to get
            addFieldsToGet(get, fields);
            
            if (version != null)
                get.setTimeRange(0, version+1); // Only retrieve data within this timerange
            if (multipleVersions) {
                get.setMaxVersions(); // Get all versions
            } else {
                get.setMaxVersions(1); // Only retrieve the most recent version of each field
            }
            
            // Retrieve the data from the repository
            result = recordTable.get(get);
            
            if (result == null || result.isEmpty())
                throw new RecordNotFoundException(newRecord(recordId));
            
            // Check if the record was deleted
            byte[] deleted = result.getValue(systemColumnFamily, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                throw new RecordNotFoundException(newRecord(recordId));
            }
        } catch (IOException e) {
            throw new RecordException("Exception occurred while retrieving record <" + recordId
                    + "> from HBase table", e);
        }
        return result;
    }
    
    private boolean recordExists(byte[] rowId, RowLock rowLock) throws IOException {
        Get get = new Get(rowId, rowLock);
        
        get.addColumn(systemColumnFamily, RecordColumn.DELETED.bytes);
        Result result = recordTable.get(get);
        if (result == null || result.isEmpty()) {
            return false;
        } else {
            byte[] deleted = result.getValue(systemColumnFamily, RecordColumn.DELETED.bytes);
            if ((deleted == null) || (Bytes.toBoolean(deleted))) {
                return false;
            } else {
                return true;
            }
        }
    }

    private Pair<String, Long> extractRecordType(Scope scope, Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            return new Pair<String, Long>(Bytes.toString(result.getValue(systemColumnFamily,
                    recordTypeIdColumnNames.get(scope))), Bytes.toLong(result.getValue(systemColumnFamily,
                    recordTypeVersionColumnNames.get(scope))));

        } else {
            // Get on version
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions = allVersionsMap
                    .get(systemColumnFamily);
            return extractVersionRecordType(version, versionableSystemCFversions, scope);
        }
    }

    private Pair<String, Long> extractVersionRecordType(Long version,
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions, Scope scope) {
        byte[] recordTypeIdColumnName = recordTypeIdColumnNames.get(scope);
        byte[] recordTypeVersionColumnName = recordTypeVersionColumnNames.get(scope);
        Entry<Long, byte[]> ceilingEntry = versionableSystemCFversions.get(recordTypeIdColumnName)
                .ceilingEntry(version);
        String recordTypeId = null;
        if (ceilingEntry != null) {
            recordTypeId = Bytes.toString(ceilingEntry.getValue());
        }
        Long recordTypeVersion = null;
        ceilingEntry = versionableSystemCFversions.get(recordTypeVersionColumnName).ceilingEntry(version);
        if (ceilingEntry != null) {
            recordTypeVersion = Bytes.toLong(ceilingEntry.getValue());
        }
        Pair<String, Long> recordType = new Pair<String, Long>(recordTypeId, recordTypeVersion);
        return recordType;
    }

    private List<Pair<QName, Object>> extractFields(Long version, Result result, ReadContext context)
            throws FieldTypeNotFoundException, RecordException, TypeException, InterruptedException {
        List<Pair<QName, Object>> fields = new ArrayList<Pair<QName, Object>>();
        NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions = result.getMap().get(columnFamily);
        if (mapWithVersions != null) {
            for (Entry<byte[], NavigableMap<Long, byte[]>> columnWithAllVersions : mapWithVersions.entrySet()) {
                NavigableMap<Long, byte[]> allValueVersions = columnWithAllVersions.getValue();
                Entry<Long, byte[]> ceilingEntry = allValueVersions.ceilingEntry(version);
                if (ceilingEntry != null) {
                    Pair<QName, Object> field = extractField(columnWithAllVersions.getKey(), ceilingEntry.getValue(),
                            context);
                    if (field != null) {
                        fields.add(field);
                    }
                }
            }
        }
        return fields;
    }

    private Pair<QName, Object> extractField(byte[] key, byte[] prefixedValue, ReadContext context)
            throws FieldTypeNotFoundException, RecordException, TypeException, InterruptedException {
        if (EncodingUtil.isDeletedField(prefixedValue)) {
            return null;
        }
        FieldType fieldType = typeManager.getFieldTypeById(key);
        context.addFieldType(fieldType);
        ValueType valueType = fieldType.getValueType();
        Object value = valueType.fromBytes(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<QName, Object>(fieldType.getName(), value);
    }

    private void addFieldsToGet(Get get, List<FieldType> fields) {
        boolean added = false;
        if (fields != null) {
            for (FieldType field : fields) {
                get.addColumn(columnFamily, ((FieldTypeImpl)field).getIdBytes());
                added = true;
            }
        }
        if (added) {
            // Add system columns explicitly to get since we're not retrieving
            // all columns
            addSystemColumnsToGet(get);
        }
    }

    private void addSystemColumnsToGet(Get get) {
        get.addColumn(systemColumnFamily, RecordColumn.DELETED.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSION.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_ID.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.NON_VERSIONED_RT_VERSION.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_RT_ID.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_RT_VERSION.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes);
        get.addColumn(systemColumnFamily, RecordColumn.VERSIONED_MUTABLE_RT_VERSION.bytes);
    }

    private boolean extractFieldsAndRecordTypes(Result result, Long version, Record record, ReadContext context)
            throws RecordTypeNotFoundException, RecordException, FieldTypeNotFoundException, TypeException,
            InterruptedException {
        List<Pair<QName, Object>> fields;
        if (version == null) {
            // All non-versioned fields are stored at version 1
            fields = extractFields(1L, result, context);
        } else {
            fields = extractFields(version, result, context);
        }
        if (fields.isEmpty()) 
            return false;
        
        for (Pair<QName, Object> field : fields) {
            record.setField(field.getV1(), field.getV2());
        }
        for (Scope readScope : context.getScopes()) {
            Pair<String, Long> recordTypePair = extractRecordType(readScope, result, version, record);
            RecordType recordType = typeManager.getRecordTypeById(recordTypePair.getV1(), recordTypePair.getV2());
            record.setRecordType(readScope, recordType.getName(), recordType.getVersion());
            context.setRecordTypeId(readScope, recordType);
        }
        return true;
    }

    public void delete(RecordId recordId) throws RecordException, RecordNotFoundException, RecordLockedException {
        ArgumentValidator.notNull(recordId, "recordId");
        long before = System.currentTimeMillis();
        org.lilyproject.repository.impl.lock.RowLock rowLock = null;
        byte[] rowId = recordId.toBytes();
        try {
            // Take Custom Lock
            rowLock = lockRow(recordId);

            if (recordExists(rowId, null)) { // Check if the record exists in the first place 
                Put put = new Put(rowId);
                put.add(systemColumnFamily, RecordColumn.DELETED.bytes, 1L, Bytes.toBytes(true));
                RecordEvent recordEvent = new RecordEvent();
                recordEvent.setType(Type.DELETE);
                
                RowLogMessage walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
                if (!rowLocker.put(put, rowLock)) {
                    throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table", null);
                }
    
                if (walMessage != null) {
                    try {
                        wal.processMessage(walMessage);
                    } catch (RowLogException e) {
                        // Processing the message failed, it will be retried later.
                    }
                }
            } else {
                throw new RecordNotFoundException(newRecord(recordId));
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while deleting record <" + recordId
                    + "> on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table",
                    e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table",
                    e);
        } finally {
            unlockRow(rowLock);
            long after = System.currentTimeMillis();
            metrics.report(Action.DELETE, (after-before));
        }
    }

    private void unlockRow(org.lilyproject.repository.impl.lock.RowLock rowLock) {
        if (rowLock != null) {
            try {
                rowLocker.unlockRow(rowLock);
            } catch (IOException e) {
                log.warn("Exception while unlocking row <" + rowLock.getRowKey()+ ">", e);
            }
        }
    }

    private org.lilyproject.repository.impl.lock.RowLock lockRow(RecordId recordId) throws IOException,
            RecordLockedException {
        return lockRow(recordId, null);
    }
    
    private org.lilyproject.repository.impl.lock.RowLock lockRow(RecordId recordId, RowLock hbaseRowLock)
            throws IOException, RecordLockedException {
        org.lilyproject.repository.impl.lock.RowLock rowLock;
        rowLock = rowLocker.lockRow(recordId.toBytes(), hbaseRowLock);
        if (rowLock == null)
            throw new RecordLockedException(recordId);
        return rowLock;
    }

    public void registerBlobStoreAccess(BlobStoreAccess blobStoreAccess) {
        blobStoreAccessRegistry.register(blobStoreAccess);
    }

    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return blobStoreAccessRegistry.getOutputStream(blob);
    }

    public InputStream getInputStream(Blob blob) throws BlobNotFoundException, BlobException {
        return blobStoreAccessRegistry.getInputStream(blob);
    }

    public void delete(Blob blob) throws BlobNotFoundException, BlobException {
        blobStoreAccessRegistry.delete(blob);
    }

    public Set<RecordId> getVariants(RecordId recordId) throws RepositoryException {
        byte[] masterRecordIdBytes = recordId.getMaster().toBytes();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(new PrefixFilter(masterRecordIdBytes));
        filterList.addFilter(new SingleColumnValueFilter(systemColumnFamily,
                RecordColumn.DELETED.bytes, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true)));

        Scan scan = new Scan(masterRecordIdBytes, filterList);
        scan.addColumn(systemColumnFamily, RecordColumn.DELETED.bytes);

        Set<RecordId> recordIds = new HashSet<RecordId>();

        try {
            ResultScanner scanner = recordTable.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                RecordId id = idGenerator.fromBytes(result.getRow());
                recordIds.add(id);
            }
            Closer.close(scanner); // Not closed in finally block: avoid HBase contact when there could be connection problems.
        } catch (IOException e) {
            throw new RepositoryException("Error getting list of variants of record " + recordId.getMaster(), e);
        }

        return recordIds;
    }
    
    private boolean checkAndProcessOpenMessages(RecordId recordId) throws RecordException, InterruptedException {
        byte[] rowKey = recordId.toBytes();
        try {
            List<RowLogMessage> messages = wal.getMessages(rowKey);
            if (messages.isEmpty())
               return true;
            for (RowLogMessage rowLogMessage : messages) {
                wal.processMessage(rowLogMessage);
            }
            return (wal.getMessages(rowKey).isEmpty());
        } catch (RowLogException e) {
            throw new RecordException("Failed to check for open WAL message for record <" + recordId + ">", e);
        }
    }

    private static class ReadContext {
        private Map<String, FieldType> fieldTypes = new HashMap<String, FieldType>();
        private Map<Scope, RecordType> recordTypes = new HashMap<Scope, RecordType>();
        private Set<Scope> scopes = new HashSet<Scope>();

        public void addFieldType(FieldType fieldType) {
            fieldTypes.put(fieldType.getId(), fieldType);
            scopes.add(fieldType.getScope());
        }

        public void setRecordTypeId(Scope scope, RecordType recordType) {
            recordTypes.put(scope, recordType);
        }

        public Map<Scope, RecordType> getRecordTypes() {
            return recordTypes;
        }

        public Map<String, FieldType> getFieldTypes() {
            return fieldTypes;
        }
        
        public Set<Scope> getScopes() {
            return scopes;
        }
    }
}

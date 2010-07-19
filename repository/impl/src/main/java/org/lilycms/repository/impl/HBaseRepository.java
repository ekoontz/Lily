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
package org.lilycms.repository.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobException;
import org.lilycms.repository.api.BlobNotFoundException;
import org.lilycms.repository.api.BlobStoreAccess;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.IdRecord;
import org.lilycms.repository.api.InvalidRecordException;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.RecordException;
import org.lilycms.repository.api.RecordExistsException;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.api.RecordNotFoundException;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.api.TypeException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.repository.api.VersionNotFoundException;
import org.lilycms.repository.impl.lock.RowLocker;
import org.lilycms.util.repo.RecordEvent;
import org.lilycms.util.repo.RecordEvent.Type;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.rowlog.impl.RowLogImpl;
import org.lilycms.rowlog.impl.RowLogProcessorImpl;
import org.lilycms.rowlog.impl.RowLogShardImpl;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.Pair;
import org.lilycms.util.io.Closer;

/**
 * Repository implementation.
 * 
 * <p>
 * After usage, the Repository should be stopped by calling the stop() method!
 */
public class HBaseRepository implements Repository {

    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("$CurrentVersion");
    private static final byte[] LOCK_COLUMN_NAME = Bytes.toBytes("$Lock");
    private static final byte[] DELETED_COLUMN_NAME = Bytes.toBytes("$Deleted");
    private static final byte[] NON_VERSIONED_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$NonVersionableRecordTypeId");
    private static final byte[] NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME = Bytes
            .toBytes("$NonVersionableRecordTypeVersion");
    private static final byte[] VERSIONED_RECORDTYPEID_COLUMN_NAME = Bytes.toBytes("$VersionableRecordTypeId");
    private static final byte[] VERSIONED_RECORDTYPEVERSION_COLUMN_NAME = Bytes
            .toBytes("$VersionableRecordTypeVersion");
    private static final byte[] VERSIONED_MUTABLE_RECORDTYPEID_COLUMN_NAME = Bytes
            .toBytes("$VersionableMutableRecordTypeId");
    private static final byte[] VERSIONED_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME = Bytes
            .toBytes("$VersionableMutableRecordTypeVersion");

    private HTableInterface recordTable;
    private final TypeManager typeManager;
    private final IdGenerator idGenerator;
    private Map<Scope, byte[]> columnFamilies = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> systemColumnFamilies = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeIdColumnNames = new HashMap<Scope, byte[]>();
    private Map<Scope, byte[]> recordTypeVersionColumnNames = new HashMap<Scope, byte[]>();
    private BlobStoreAccessRegistry blobStoreAccessRegistry;
    private RowLog wal;
    private RowLog messageQueue;
    private RowLogShard messageQueueShard;
    private RowLogProcessor messageQueueProcessor;
    private RowLocker rowLocker;
    private String zkConnectString;

    public HBaseRepository(TypeManager typeManager, IdGenerator idGenerator,
            BlobStoreAccessFactory blobStoreOutputStreamFactory, Configuration configuration) throws IOException {
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        blobStoreAccessRegistry = new BlobStoreAccessRegistry();
        blobStoreAccessRegistry.setBlobStoreOutputStreamFactory(blobStoreOutputStreamFactory);
        recordTable = HBaseTableUtil.getRecordTable(configuration);

        columnFamilies.put(Scope.NON_VERSIONED, HBaseTableUtil.NON_VERSIONED_COLUMN_FAMILY);
        columnFamilies.put(Scope.VERSIONED, HBaseTableUtil.VERSIONED_COLUMN_FAMILY);
        columnFamilies.put(Scope.VERSIONED_MUTABLE, HBaseTableUtil.VERSIONED_MUTABLE_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.NON_VERSIONED, HBaseTableUtil.NON_VERSIONED_SYSTEM_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.VERSIONED, HBaseTableUtil.VERSIONED_SYSTEM_COLUMN_FAMILY);
        systemColumnFamilies.put(Scope.VERSIONED_MUTABLE, HBaseTableUtil.VERSIONED_SYSTEM_COLUMN_FAMILY);
        recordTypeIdColumnNames.put(Scope.NON_VERSIONED, NON_VERSIONED_RECORDTYPEID_COLUMN_NAME);
        recordTypeIdColumnNames.put(Scope.VERSIONED, VERSIONED_RECORDTYPEID_COLUMN_NAME);
        recordTypeIdColumnNames.put(Scope.VERSIONED_MUTABLE, VERSIONED_MUTABLE_RECORDTYPEID_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.NON_VERSIONED, NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.VERSIONED, VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        recordTypeVersionColumnNames.put(Scope.VERSIONED_MUTABLE, VERSIONED_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);

        // Initialize Wal and Message Queue
        initializeMessageQueue(configuration);
        initializeWal(configuration);

        // Start Message Queue Processor
        zkConnectString = configuration.get("hbase.zookeeper.quorum") + ":" + configuration.get("hbase.zookeeper.property.clientPort");
        messageQueueProcessor = new RowLogProcessorImpl(messageQueue, messageQueueShard, zkConnectString);
        messageQueueProcessor.start();

        rowLocker = new RowLocker(recordTable, HBaseTableUtil.NON_VERSIONED_SYSTEM_COLUMN_FAMILY, LOCK_COLUMN_NAME,
                10000);
    }

    public void stop() {
        messageQueueProcessor.stop();
    }

    private void initializeMessageQueue(Configuration configuration) throws IOException {
        messageQueue = new RowLogImpl("MQ", recordTable, HBaseTableUtil.MQ_PAYLOAD_COLUMN_FAMILY,
                HBaseTableUtil.MQ_COLUMN_FAMILY, 10000L, zkConnectString); 
        messageQueueShard = new RowLogShardImpl("MQS1", configuration, messageQueue);
        messageQueue.registerShard(messageQueueShard);
        messageQueue.registerConsumer(new DevNull());
    }

    private void initializeWal(Configuration configuration) throws IOException {
        wal = new RowLogImpl("WAL", recordTable, HBaseTableUtil.WAL_PAYLOAD_COLUMN_FAMILY, HBaseTableUtil.WAL_COLUMN_FAMILY, 10000L, zkConnectString);
        // Work with only one shard for now
        RowLogShard walShard = new RowLogShardImpl("WS1", configuration, wal);
        wal.registerShard(walShard);
        wal.registerConsumer(new MessageQueueFeeder(messageQueue));
    }

    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }

    public RowLog getWal() {
        return wal;
    }

    public RowLog getMessageQueue() {
        return messageQueue;
    }

    public Record newRecord() {
        return new RecordImpl();
    }

    public Record newRecord(RecordId recordId) {
        ArgumentValidator.notNull(recordId, "recordId");
        return new RecordImpl(recordId);
    }

    public Record create(Record record) throws RecordExistsException, RecordNotFoundException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException {

        Record newRecord = record.clone();

        RecordId recordId = newRecord.getId();
        if (recordId == null) {
            recordId = idGenerator.newRecordId();
            newRecord.setId(recordId);
        }

        byte[] rowId = recordId.toBytes();
        RowLock rowLock = null;
        RowLogMessage walMessage;
        try {
            // Take HBase RowLock
            rowLock = recordTable.lockRow(rowId);
            if (rowLock == null)
                throw new RecordException("Failed to lock row while creating record <" + recordId
                        + "> in HBase table", null);

            checkCreatePreconditions(record);

            // Check if a previous incarnation of the record existed and clear data if needed
            reincarnateRecord(newRecord, rowId, rowLock);
            
            Record dummyOriginalRecord = newRecord();
            Put put = new Put(newRecord.getId().toBytes(), rowLock);
            put.add(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME, Bytes.toBytes(false));
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.CREATE);
            calculateRecordChanges(newRecord, dummyOriginalRecord, 1L, put, recordEvent);
            // Make sure the record type changed flag stays false for a newly
            // created record
            recordEvent.setRecordTypeChanged(false);
            if (newRecord.getVersion() != null)
                recordEvent.setVersionCreated(newRecord.getVersion());

            walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
            recordTable.put(put);
        } catch (IOException e) {
            throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                    e);
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                    e);
        } finally {
            if (rowLock != null) {
                try {
                    recordTable.unlockRow(rowLock);
                } catch (IOException e) {
                    // Ignore for now
                }
            }
        }

        // Take Custom RowLock
        org.lilycms.repository.impl.lock.RowLock customRowLock = null;
        try {
            customRowLock = rowLocker.lockRow(rowId);
            if (customRowLock != null) {
                try {
                    wal.processMessage(walMessage);
                } catch (RowLogException e) {
                    // Processing the message failed, it will be retried later
                }
            }
        } catch (IOException e) {
            throw new RecordException("Exception occurred while creating record <" + recordId + "> in HBase table",
                    e);
        } finally {
            if (customRowLock != null) {
                try {
                    rowLocker.unlockRow(customRowLock);
                } catch (IOException e) {
                    // Ignore for now
                }
            }
        }
        return newRecord;
    }

    private void reincarnateRecord(Record newRecord, byte[] rowId, RowLock rowLock) throws IOException,
            RecordExistsException {
        Get get = new Get(rowId, rowLock);
        if (recordTable.exists(get)) {
            get.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME);
            get.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), CURRENT_VERSION_COLUMN_NAME);
            Result result = recordTable.get(get);
            if (result == null || result.isEmpty()) {
                throw new RecordExistsException(newRecord);
            } else {
                byte[] deleted = result.getValue(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME);
                if ((deleted != null) && (Bytes.toBoolean(deleted))) {
                    if (null != result.getValue(systemColumnFamilies.get(Scope.NON_VERSIONED), CURRENT_VERSION_COLUMN_NAME)) {
                        clearData(rowId, rowLock);
                    }
                } else {
                    throw new RecordExistsException(newRecord);
                }
            }
        }
    }

    private void clearData(byte[] rowId, RowLock rowLock) throws IOException {
        Delete delete = new Delete(rowId, -1, rowLock);
        delete.deleteFamily(columnFamilies.get(Scope.NON_VERSIONED));
        delete.deleteFamily(columnFamilies.get(Scope.VERSIONED));
        delete.deleteFamily(columnFamilies.get(Scope.VERSIONED_MUTABLE));
        delete.deleteColumns(systemColumnFamilies.get(Scope.NON_VERSIONED), CURRENT_VERSION_COLUMN_NAME);
        delete.deleteColumns(systemColumnFamilies.get(Scope.NON_VERSIONED), NON_VERSIONED_RECORDTYPEID_COLUMN_NAME);
        delete.deleteColumns(systemColumnFamilies.get(Scope.NON_VERSIONED), NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        delete.deleteColumns(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_RECORDTYPEID_COLUMN_NAME);
        delete.deleteColumns(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        delete.deleteColumns(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_MUTABLE_RECORDTYPEID_COLUMN_NAME);
        delete.deleteColumns(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);
        recordTable.delete(delete);
    }

    private void checkCreatePreconditions(Record record) throws InvalidRecordException {
        ArgumentValidator.notNull(record, "record");
        if (record.getRecordTypeId() == null) {
            throw new InvalidRecordException(record, "The recordType cannot be null for a record to be created.");
        }
        if (record.getFields().isEmpty()) {
            throw new InvalidRecordException(record, "Creating an empty record is not allowed");
        }
    }

    public Record update(Record record) throws RecordNotFoundException, InvalidRecordException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        byte[] rowId = recordId.toBytes();
        org.lilycms.repository.impl.lock.RowLock rowLock = null;
        RowLogMessage walMessage;
        try {
            // Take Custom Lock
            rowLock = rowLocker.lockRow(rowId);
            if (rowLock == null)
                throw new RecordException("Failed to lock row while updating record <" + recordId
                        + "> in HBase table", null);

            checkUpdatePreconditions(record);

            Record originalRecord = read(newRecord.getId(), null, null, new ReadContext(false));

            Put put = new Put(newRecord.getId().toBytes());
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            long newVersion = originalRecord.getVersion() == null ? 1 : originalRecord.getVersion() + 1;
            if (calculateRecordChanges(newRecord, originalRecord, newVersion, put, recordEvent)) {
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
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while putting updated record <" + recordId
                    + "> on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId + "> on HBase table",
                    e);
        } finally {
            if (rowLock != null) {
                try {
                    rowLocker.unlockRow(rowLock);
                } catch (IOException e) {
                    // Ignore for now
                }
            }
        }
        return newRecord;
    }

    private void checkUpdatePreconditions(Record record) throws InvalidRecordException, RecordNotFoundException,
            RecordException {
        ArgumentValidator.notNull(record, "record");
        if (record.getRecordTypeId() == null) {
            throw new InvalidRecordException(record, "The recordType cannot be null for a record to be updated.");
        }
    }

    // Calculates the changes that are to be made on the record-row and puts
    // this information on the Put object and the RecordEvent
    private boolean calculateRecordChanges(Record record, Record originalRecord, Long version, Put put,
            RecordEvent recordEvent) throws RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, TypeException {
        String recordTypeId = record.getRecordTypeId();
        Long recordTypeVersion = record.getRecordTypeVersion();

        RecordType recordType = typeManager.getRecordType(recordTypeId, recordTypeVersion);

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
            recordTypeVersion = recordType.getVersion();
            if (!recordTypeId.equals(originalRecord.getRecordTypeId())
                    || !recordTypeVersion.equals(originalRecord.getRecordTypeVersion())) {
                recordEvent.setRecordTypeChanged(true);
                put.add(systemColumnFamilies.get(Scope.NON_VERSIONED), NON_VERSIONED_RECORDTYPEID_COLUMN_NAME, Bytes
                        .toBytes(recordTypeId));
                put.add(systemColumnFamilies.get(Scope.NON_VERSIONED), NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME,
                        Bytes.toBytes(recordTypeVersion));
            }
            // Always set the record type on the record since the requested
            // record type could have been given without a version number
            record.setRecordType(recordTypeId, recordTypeVersion);
            if (version != null) {
                put.add(systemColumnFamilies.get(Scope.NON_VERSIONED), CURRENT_VERSION_COLUMN_NAME, Bytes
                        .toBytes(version));
            }

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

    private Set<Scope> calculateChangedFields(Record record, Record originalRecord, RecordType recordType,
            Long version, Put put, RecordEvent recordEvent) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException {
        Set<Scope> changedScopes = calculateUpdateAndDeleteFields(record.getFields(), record.getFieldsToDelete(),
                originalRecord.getFields(), version, put, recordEvent);
        for (Scope scope : changedScopes) {
            if (Scope.NON_VERSIONED.equals(scope)) {
                put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), Bytes.toBytes(recordType
                        .getId()));
                put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), Bytes
                        .toBytes(recordType.getVersion()));
            } else {
                put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getVersion()));

            }
            record.setRecordType(scope, recordType.getId(), recordType.getVersion());
        }
        return changedScopes;
    }

    private Set<Scope> calculateUpdateAndDeleteFields(Map<QName, Object> fields, List<QName> fieldsToDelete,
            Map<QName, Object> originalFields, Long version, Put put, RecordEvent recordEvent)
            throws FieldTypeNotFoundException, RecordTypeNotFoundException, RecordException, TypeException {
        // Update fields
        Set<Scope> changedScopes = new HashSet<Scope>();
        changedScopes.addAll(calculateUpdateFields(fields, originalFields, version, put, recordEvent));
        // Delete fields
        changedScopes.addAll(calculateDeleteFields(fieldsToDelete, originalFields, version, put, recordEvent));
        return changedScopes;
    }

    private Set<Scope> calculateUpdateFields(Map<QName, Object> fields, Map<QName, Object> originalFields,
            Long version, Put put, RecordEvent recordEvent) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException {
        Set<Scope> changedScopes = new HashSet<Scope>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            QName fieldName = field.getKey();
            Object newValue = field.getValue();
            Object originalValue = originalFields.get(fieldName);
            if (((newValue == null) && (originalValue != null)) || !newValue.equals(originalValue)) {
                FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
                Scope scope = fieldType.getScope();
                byte[] fieldIdAsBytes = Bytes.toBytes(fieldType.getId());
                byte[] encodedFieldValue = encodeFieldValue(fieldType, newValue);

                if (Scope.NON_VERSIONED.equals(scope)) {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, encodedFieldValue);
                } else {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, version, encodedFieldValue);
                }
                changedScopes.add(scope);

                recordEvent.addUpdatedField(fieldType.getId());
            }
        }
        return changedScopes;
    }

    private byte[] encodeFieldValue(FieldType fieldType, Object fieldValue) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException {
        ValueType valueType = fieldType.getValueType();

        // TODO validate with Class#isAssignableFrom()
        byte[] encodedFieldValue = valueType.toBytes(fieldValue);
        encodedFieldValue = EncodingUtil.prefixValue(encodedFieldValue, EncodingUtil.EXISTS_FLAG);
        return encodedFieldValue;
    }

    private Set<Scope> calculateDeleteFields(List<QName> fieldsToDelete, Map<QName, Object> originalFields,
            Long version, Put put, RecordEvent recordEvent) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException {
        Set<Scope> changedScopes = new HashSet<Scope>();
        for (QName fieldToDelete : fieldsToDelete) {
            if (originalFields.get(fieldToDelete) != null) {
                FieldType fieldType = typeManager.getFieldTypeByName(fieldToDelete);
                Scope scope = fieldType.getScope();
                byte[] fieldIdAsBytes = Bytes.toBytes(fieldType.getId());
                if (Scope.NON_VERSIONED.equals(scope)) {
                    put.add(columnFamilies.get(scope), fieldIdAsBytes, new byte[] { EncodingUtil.DELETE_FLAG });
                } else {
                    put
                            .add(columnFamilies.get(scope), fieldIdAsBytes, version,
                                    new byte[] { EncodingUtil.DELETE_FLAG });
                }
                changedScopes.add(scope);

                recordEvent.addUpdatedField(fieldType.getId());
            }
        }
        return changedScopes;
    }

    public Record updateMutableFields(Record record) throws InvalidRecordException, RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        Record newRecord = record.clone();

        RecordId recordId = record.getId();
        byte[] rowId = recordId.toBytes();
        org.lilycms.repository.impl.lock.RowLock rowLock = null;
        RowLogMessage walMessage;
        try {
            // Take Custom Lock
            rowLock = rowLocker.lockRow(rowId);
            if (rowLock == null)
                throw new RecordException("Failed to lock row while updating record <" + recordId
                        + "> in HBase table", null);

            checkUpdatePreconditions(record);
            Long version = record.getVersion();
            if (version == null) {
                throw new InvalidRecordException(record,
                        "The version of the record cannot be null to update mutable fields");
            }

            Record originalRecord = read(record.getId(), version, null, new ReadContext(false));

            // Update the mutable fields
            Put put = new Put(record.getId().toBytes());
            Map<QName, Object> fieldsToUpdate = filterMutableFields(record.getFields());
            Map<QName, Object> originalFields = filterMutableFields(originalRecord.getFields());
            RecordEvent recordEvent = new RecordEvent();
            recordEvent.setType(Type.UPDATE);
            recordEvent.setVersionUpdated(version);

            Set<Scope> changedScopes = calculateUpdateFields(fieldsToUpdate, originalFields, version, put, recordEvent);

            // Delete mutable fields and copy values to the next version if
            // needed
            Record originalNextRecord = null;
            try {
                originalNextRecord = read(record.getId(), version + 1, null, new ReadContext(false));
            } catch (RecordNotFoundException exception) {
                // There is no next record
            }
            List<QName> fieldsToDelete = filterMutableFieldsToDelete(record.getFieldsToDelete());
            Map<QName, Object> originalNextFields = new HashMap<QName, Object>();
            if (originalNextRecord != null) {
                originalNextFields.putAll(filterMutableFields(originalNextRecord.getFields()));
            }
            boolean deleted = calculateDeleteMutableFields(fieldsToDelete, originalFields, originalNextFields, version,
                    put, recordEvent);

            Scope scope = Scope.VERSIONED_MUTABLE;
            if (!changedScopes.isEmpty() || deleted) {
                RecordType recordType = typeManager.getRecordType(record.getRecordTypeId(), record
                        .getRecordTypeVersion());
                // Update the mutable record type
                put.add(systemColumnFamilies.get(scope), recordTypeIdColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getId()));
                put.add(systemColumnFamilies.get(scope), recordTypeVersionColumnNames.get(scope), version, Bytes
                        .toBytes(recordType.getVersion()));

                recordEvent.setVersionUpdated(version);

                walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
                if (!rowLocker.put(put, rowLock)) {
                    throw new RecordException("Exception occurred while putting updated record <" + recordId
                            + "> on HBase table", null);
                }
                newRecord.setRecordType(scope, recordType.getId(), recordType.getVersion());
                if (walMessage != null) {
                    try {
                        wal.processMessage(walMessage);
                    } catch (RowLogException e) {
                        // Processing the message failed, it will be retried later
                    }
                }
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while putting updated record <" + record.getId()
                    + "> on HBase table", e);
        } catch (IOException e) {
            throw new RecordException("Exception occurred while updating record <" + recordId + "> in HBase table",
                    e);
        } finally {
            if (rowLock != null) {
                try {
                    rowLocker.unlockRow(rowLock);
                } catch (IOException e) {
                    // Ignore for now
                }
            }
        }
        return newRecord;
    }

    private Map<QName, Object> filterMutableFields(Map<QName, Object> fields) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException {
        Map<QName, Object> mutableFields = new HashMap<QName, Object>();
        for (Entry<QName, Object> field : fields.entrySet()) {
            FieldType fieldType = typeManager.getFieldTypeByName(field.getKey());
            if (Scope.VERSIONED_MUTABLE.equals(fieldType.getScope())) {
                mutableFields.put(field.getKey(), field.getValue());
            }
        }
        return mutableFields;
    }

    private List<QName> filterMutableFieldsToDelete(List<QName> fields) throws FieldTypeNotFoundException,
            RecordTypeNotFoundException, RecordException, TypeException {
        List<QName> mutableFields = new ArrayList<QName>();
        for (QName field : fields) {
            FieldType fieldType = typeManager.getFieldTypeByName(field);
            if (Scope.VERSIONED_MUTABLE.equals(fieldType.getScope())) {
                mutableFields.add(field);
            }
        }
        return mutableFields;
    }

    private boolean calculateDeleteMutableFields(List<QName> fieldsToDelete, Map<QName, Object> originalFields,
            Map<QName, Object> originalNextFields, Long version, Put put, RecordEvent recordEvent)
            throws FieldTypeNotFoundException, RecordTypeNotFoundException, RecordException, TypeException {
        boolean changed = false;
        for (QName fieldToDelete : fieldsToDelete) {
            Object originalValue = originalFields.get(fieldToDelete);
            if (originalValue != null) {
                FieldType fieldType = typeManager.getFieldTypeByName(fieldToDelete);
                byte[] fieldIdBytes = Bytes.toBytes(fieldType.getId());
                put.add(columnFamilies.get(Scope.VERSIONED_MUTABLE), fieldIdBytes, version,
                        new byte[] { EncodingUtil.DELETE_FLAG });
                // Copy original value to the next record if that record had the
                // same value
                if (originalValue.equals(originalNextFields.get(fieldToDelete))) {
                    byte[] encodedValue = encodeFieldValue(fieldType, originalValue);
                    put.add(columnFamilies.get(Scope.VERSIONED_MUTABLE), fieldIdBytes, version + 1, encodedValue);
                }
                recordEvent.addUpdatedField(fieldType.getId());
                changed = true;
            }
        }
        return changed;
    }

    public Record read(RecordId recordId) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        return read(recordId, null, null);
    }

    public Record read(RecordId recordId, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        return read(recordId, null, fieldNames);
    }

    public Record read(RecordId recordId, Long version) throws RecordNotFoundException, RecordTypeNotFoundException,
            FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        return read(recordId, version, null);
    }

    public Record read(RecordId recordId, Long version, List<QName> fieldNames) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        ReadContext readContext = new ReadContext(false);

        List<FieldType> fields = null;
        if (fieldNames != null) {
            fields = new ArrayList<FieldType>();
            for (QName fieldName : fieldNames) {
                fields.add(typeManager.getFieldTypeByName(fieldName));
            }
        }

        return read(recordId, version, fields, readContext);
    }

    public IdRecord readWithIds(RecordId recordId, Long version, List<String> fieldIds) throws RecordNotFoundException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, VersionNotFoundException, TypeException {
        ReadContext readContext = new ReadContext(true);

        List<FieldType> fields = null;
        if (fieldIds != null) {
            fields = new ArrayList<FieldType>(fieldIds.size());
            for (String fieldId : fieldIds) {
                fields.add(typeManager.getFieldTypeById(fieldId));
            }
        }

        Record record = read(recordId, version, fields, readContext);

        Map<String, QName> idToQNameMapping = new HashMap<String, QName>();
        for (FieldType fieldType : readContext.getFieldTypes().values()) {
            idToQNameMapping.put(fieldType.getId(), fieldType.getName());
        }

        return new IdRecordImpl(record, idToQNameMapping);
    }

    private Record read(RecordId recordId, Long requestedVersion, List<FieldType> fields, ReadContext readContext)
            throws RecordNotFoundException, RecordTypeNotFoundException, FieldTypeNotFoundException,
            RecordException, VersionNotFoundException, TypeException {
        ArgumentValidator.notNull(recordId, "recordId");

        Result result = getRow(recordId, requestedVersion, fields);

        Record record = newRecord(recordId);

        // Set retrieved version on the record
        extractVersion(requestedVersion, result, record);

        // Extract the actual fields from the retrieved data
        if (extractFields(result, record.getVersion(), record, readContext)) {
            // Set the recordType explicitly in case only versioned fields were
            // extracted
            Pair<String, Long> recordTypePair = extractRecordType(Scope.NON_VERSIONED, result, null, record);
            record.setRecordType(recordTypePair.getV1(), recordTypePair.getV2());
        }
        return record;
    }

    private void extractVersion(Long requestedVersion, Result result, Record record) throws VersionNotFoundException {
        byte[] latestVersionBytes = result.getValue(systemColumnFamilies.get(Scope.NON_VERSIONED),
                CURRENT_VERSION_COLUMN_NAME);
        Long latestVersion = latestVersionBytes != null ? Bytes.toLong(latestVersionBytes) : null;
        if (requestedVersion != null) {
            record.setVersion(requestedVersion);
            if (latestVersion == null || latestVersion < requestedVersion) {
                throw new VersionNotFoundException(record);
            }
        } else {
            record.setVersion(latestVersion);
        }
    }

    // Retrieves the row from the table and check if it exists and has not been flagged as deleted
    private Result getRow(RecordId recordId, Long requestedVersion, List<FieldType> fields)
            throws RecordNotFoundException, RecordException {
        Result result;
        Get get = new Get(recordId.toBytes());
        if (requestedVersion != null) {
            get.setMaxVersions();
        }
        // Add the columns for the fields to get
        addFieldsToGet(get, fields);
        try {
            if (!recordTable.exists(new Get(recordId.toBytes()))) {
                throw new RecordNotFoundException(newRecord(recordId));
            }
            
            // Retrieve the data from the repository
            result = recordTable.get(get);
            
            // Check if the record was deleted
            byte[] deleted = result.getValue(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME);
            if ((deleted != null) && (Bytes.toBoolean(deleted))) {
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
        if (!recordTable.exists(get)) return false;
        
        get.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME);
        Result result = recordTable.get(get);
        if (result == null || result.isEmpty()) {
            return true;
        } else {
            byte[] deleted = result.getValue(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME);
            if ((deleted != null) && (Bytes.toBoolean(deleted))) {
                return false;
            } else {
                return true;
            }
        }
    }

    private Pair<String, Long> extractRecordType(Scope scope, Result result, Long version, Record record) {
        if (version == null) {
            // Get latest version
            return new Pair<String, Long>(Bytes.toString(result.getValue(systemColumnFamilies.get(scope),
                    recordTypeIdColumnNames.get(scope))), Bytes.toLong(result.getValue(systemColumnFamilies.get(scope),
                    recordTypeVersionColumnNames.get(scope))));

        } else {
            // Get on version
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> versionableSystemCFversions = allVersionsMap
                    .get(systemColumnFamilies.get(scope));
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

    private List<Pair<QName, Object>> extractFields(NavigableMap<byte[], byte[]> familyMap, ReadContext context)
            throws FieldTypeNotFoundException, RecordException, TypeException {
        List<Pair<QName, Object>> fields = new ArrayList<Pair<QName, Object>>();
        if (familyMap != null) {
            for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                Pair<QName, Object> field = extractField(entry.getKey(), entry.getValue(), context);
                if (field != null) {
                    fields.add(field);
                }
            }
        }
        return fields;
    }

    private List<Pair<QName, Object>> extractVersionFields(Long version,
            NavigableMap<byte[], NavigableMap<Long, byte[]>> mapWithVersions, ReadContext context)
            throws FieldTypeNotFoundException, RecordException, TypeException {
        List<Pair<QName, Object>> fields = new ArrayList<Pair<QName, Object>>();
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
            throws FieldTypeNotFoundException, RecordException, TypeException {
        if (EncodingUtil.isDeletedField(prefixedValue)) {
            return null;
        }
        String fieldId = Bytes.toString(key);
        FieldType fieldType = typeManager.getFieldTypeById(fieldId);
        context.addFieldType(fieldType);
        ValueType valueType = fieldType.getValueType();
        Object value = valueType.fromBytes(EncodingUtil.stripPrefix(prefixedValue));
        return new Pair<QName, Object>(fieldType.getName(), value);
    }

    private void addFieldsToGet(Get get, List<FieldType> fields) {
        boolean added = false;
        if (fields != null) {
            for (FieldType field : fields) {
                get.addColumn(columnFamilies.get(field.getScope()), Bytes.toBytes(field.getId()));
            }
            added = true;
        }
        if (added) {
            // Add system columns explicitly to get since we're not retrieving
            // all columns
            addSystemColumnsToGet(get);
        }
    }

    private void addSystemColumnsToGet(Get get) {
        get.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME);
        get.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), CURRENT_VERSION_COLUMN_NAME);
        get.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), NON_VERSIONED_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), NON_VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        get.addColumn(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_RECORDTYPEVERSION_COLUMN_NAME);
        get.addColumn(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_MUTABLE_RECORDTYPEID_COLUMN_NAME);
        get.addColumn(systemColumnFamilies.get(Scope.VERSIONED), VERSIONED_MUTABLE_RECORDTYPEVERSION_COLUMN_NAME);
    }

    private boolean extractFields(Result result, Long version, Record record, ReadContext context)
            throws RecordTypeNotFoundException, FieldTypeNotFoundException, RecordException, TypeException {
        boolean nvExtracted = extractFields(Scope.NON_VERSIONED, result, null, record, context);
        boolean vExtracted = false;
        boolean vmExtracted = false;
        if (version != null) {
            vExtracted = extractFields(Scope.VERSIONED, result, version, record, context);
            vmExtracted = extractFields(Scope.VERSIONED_MUTABLE, result, version, record, context);
        }
        return nvExtracted || vExtracted || vmExtracted;
    }

    private boolean extractFields(Scope scope, Result result, Long version, Record record, ReadContext context)
            throws RecordTypeNotFoundException, RecordException, FieldTypeNotFoundException, TypeException {
        boolean retrieved = false;
        List<Pair<QName, Object>> fields;
        if (version == null) {
            fields = extractFields(result.getFamilyMap(columnFamilies.get(scope)), context);
        } else {
            fields = extractVersionFields(version, result.getMap().get(columnFamilies.get(scope)), context);
        }
        if (!fields.isEmpty()) {
            for (Pair<QName, Object> field : fields) {
                record.setField(field.getV1(), field.getV2());
            }
            Pair<String, Long> recordTypePair = extractRecordType(scope, result, version, record);
            record.setRecordType(scope, recordTypePair.getV1(), recordTypePair.getV2());
            retrieved = true;
        }
        return retrieved;
    }

    public void delete(RecordId recordId) throws RecordException {
        ArgumentValidator.notNull(recordId, "recordId");
        
        org.lilycms.repository.impl.lock.RowLock rowLock = null;
        byte[] rowId = recordId.toBytes();
        try {
            // Take Custom Lock
            rowLock = rowLocker.lockRow(rowId);
            if (rowLock == null)
                throw new RecordException("Failed to lock row while updating record <" + recordId
                        + "> in HBase table", null);

            if (recordExists(rowId, null)) { // Check if the record exists in the first place 
                Put put = new Put(rowId);
                put.add(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME, Bytes.toBytes(true));
                RecordEvent recordEvent = new RecordEvent();
                recordEvent.setType(Type.DELETE);
                
                RowLogMessage walMessage = wal.putMessage(recordId.toBytes(), null, recordEvent.toJsonBytes(), put);
                if (!rowLocker.put(put, rowLock)) {
                    throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table", null);
                }
                clearData(rowId, null);
    
                if (walMessage != null) {
                    try {
                        wal.processMessage(walMessage);
                    } catch (RowLogException e) {
                        // Processing the message failed, it will be retried later.
                    }
                }
            }
        } catch (RowLogException e) {
            throw new RecordException("Exception occurred while deleting record <" + recordId
                    + "> on HBase table", e);

        } catch (IOException e) {
            throw new RecordException("Exception occurred while deleting record <" + recordId + "> on HBase table",
                    e);
        } finally {
            if (rowLock != null) {
                try {
                    rowLocker.unlockRow(rowLock);
                } catch (IOException e) {
                    // Ignore for now
                }
            }
        }
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
        filterList.addFilter(new FirstKeyOnlyFilter());
        filterList.addFilter(new PrefixFilter(masterRecordIdBytes));
        filterList.addFilter(new SingleColumnValueFilter(systemColumnFamilies.get(Scope.NON_VERSIONED),
                DELETED_COLUMN_NAME, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(true)));
        
        Scan scan = new Scan(masterRecordIdBytes, filterList);
        scan.addColumn(systemColumnFamilies.get(Scope.NON_VERSIONED), DELETED_COLUMN_NAME);

        Set<RecordId> recordIds = new HashSet<RecordId>();

        ResultScanner scanner = null;
        try {
            scanner = recordTable.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                RecordId id = idGenerator.fromBytes(result.getRow());
                recordIds.add(id);
            }
        } catch (IOException e) {
            throw new RepositoryException("Error getting list of variants of record " + recordId.getMaster(), e);
        } finally {
            Closer.close(scanner);
        }

        return recordIds;
    }

    private static class ReadContext {
        private Map<String, FieldType> fieldTypes;

        public ReadContext(boolean collectFieldTypes) {
            if (collectFieldTypes) {
                this.fieldTypes = new HashMap<String, FieldType>();
            }
        }

        public void addFieldType(FieldType fieldType) {
            if (fieldTypes != null) {
                this.fieldTypes.put(fieldType.getId(), fieldType);
            }
        }

        public Map<String, FieldType> getFieldTypes() {
            return fieldTypes;
        }
    }
}

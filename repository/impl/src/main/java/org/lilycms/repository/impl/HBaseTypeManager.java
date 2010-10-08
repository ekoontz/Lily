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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.FieldTypeEntry;
import org.lilycms.repository.api.FieldTypeExistsException;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.FieldTypeUpdateException;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeExistsException;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.api.TypeException;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.hbase.LocalHTable;
import org.lilycms.util.io.Closer;
import org.lilycms.util.zookeeper.ZkUtil;
import org.lilycms.util.zookeeper.ZooKeeperItf;

public class HBaseTypeManager extends AbstractTypeManager implements TypeManager {

    private Log log = LogFactory.getLog(getClass());

    private static final byte[] TYPE_TABLE = Bytes.toBytes("typeTable");
    private static final byte[] NON_VERSIONED_COLUMN_FAMILY = Bytes.toBytes("NVCF");
    private static final byte[] FIELDTYPEENTRY_COLUMN_FAMILY = Bytes.toBytes("FTECF");
    private static final byte[] MIXIN_COLUMN_FAMILY = Bytes.toBytes("MCF");

    private static final byte[] CURRENT_VERSION_COLUMN_NAME = Bytes.toBytes("$currentVersion");

    private static final byte[] RECORDTYPE_NAME_COLUMN_NAME = Bytes.toBytes("$rtname");
    private static final byte[] FIELDTYPE_NAME_COLUMN_NAME = Bytes.toBytes("$ftname");
    private static final byte[] FIELDTYPE_VALUETYPE_COLUMN_NAME = Bytes.toBytes("$valueType");
    private static final byte[] FIELDTPYE_SCOPE_COLUMN_NAME = Bytes.toBytes("$scope");
    private static final byte[] CONCURRENT_COUNTER_COLUMN_NAME = Bytes.toBytes("$cc");

    private static final String CACHE_INVALIDATION_PATH = "/lily/typemanager/cache";

    private HTableInterface typeTable;
    private Map<QName, FieldType> fieldTypeNameCache = new HashMap<QName, FieldType>();
    private Map<QName, RecordType> recordTypeNameCache = new HashMap<QName, RecordType>();
    private Map<String, FieldType> fieldTypeIdCache = new HashMap<String, FieldType>();
    private Map<String, RecordType> recordTypeIdCache = new HashMap<String, RecordType>();
    private final ZooKeeperItf zooKeeper;
    private final CacheWatcher cacheWatcher = new CacheWatcher();

    public HBaseTypeManager(IdGenerator idGenerator, Configuration configuration, ZooKeeperItf zooKeeper)
            throws IOException, InterruptedException, KeeperException {
        this.idGenerator = idGenerator;
        this.zooKeeper = zooKeeper;

        HBaseAdmin admin = new HBaseAdmin(configuration);
        try {
            admin.getTableDescriptor(TYPE_TABLE);
        } catch (TableNotFoundException e) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TYPE_TABLE);
            tableDescriptor.addFamily(new HColumnDescriptor(NON_VERSIONED_COLUMN_FAMILY));
            tableDescriptor.addFamily(new HColumnDescriptor(FIELDTYPEENTRY_COLUMN_FAMILY, HConstants.ALL_VERSIONS,
                    "none", false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
            tableDescriptor.addFamily(new HColumnDescriptor(MIXIN_COLUMN_FAMILY, HConstants.ALL_VERSIONS, "none",
                    false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER));
            admin.createTable(tableDescriptor);
        }

        this.typeTable = new LocalHTable(configuration, TYPE_TABLE);
        registerDefaultValueTypes();

        ZkUtil.createPath(zooKeeper, CACHE_INVALIDATION_PATH);
        zooKeeper.addDefaultWatcher(new ConnectionWatcher());
        initializeCaches();
    }

    private class CacheWatcher implements Watcher {
        public void process(WatchedEvent event) {
            new Thread() {
                public void run() {
                    try {
                        initializeCaches();
                    } catch (InterruptedException e) {
                        // Stop
                    }
                };
            }.start();
        }
    }

    private class ConnectionWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (EventType.None.equals(event.getType()) && KeeperState.SyncConnected.equals(event.getState())) {
                new Thread() {
                    public void run() {
                        try {
                            initializeCaches();
                        } catch (InterruptedException e) {
                            // Stop
                        }
                    }
                }.start();
            }
        }
    }

    public void close() throws IOException {
    }

    public RecordType createRecordType(RecordType recordType) throws RecordTypeExistsException,
            RecordTypeNotFoundException, FieldTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(recordType, "recordType");
        RecordType newRecordType = recordType.clone();
        Long recordTypeVersion = Long.valueOf(1);
        try {
            UUID uuid = getValidUUID();
            byte[] rowId = idToBytes(uuid);
            // Take a counter on a row with the name as key
            byte[] nameBytes = encodeName(recordType.getName());
            long concurrentCounter = getTypeTable().incrementColumnValue(nameBytes, NON_VERSIONED_COLUMN_FAMILY,
                    CONCURRENT_COUNTER_COLUMN_NAME, 1);

            if (getRecordTypeFromCache(recordType.getName()) != null) 
                throw new RecordTypeExistsException(recordType);

            Put put = new Put(rowId);
            put.add(NON_VERSIONED_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(recordTypeVersion));
            put.add(NON_VERSIONED_COLUMN_FAMILY, RECORDTYPE_NAME_COLUMN_NAME, nameBytes);

            Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
            for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
                putFieldTypeEntry(recordTypeVersion, put, fieldTypeEntry);
            }

            Map<String, Long> mixins = recordType.getMixins();
            for (Entry<String, Long> mixin : mixins.entrySet()) {
                newRecordType.addMixin(mixin.getKey(), putMixinOnRecordType(recordTypeVersion, put, mixin.getKey(),
                        mixin.getValue()));
            }

            if (!getTypeTable().checkAndPut(nameBytes, NON_VERSIONED_COLUMN_FAMILY, CONCURRENT_COUNTER_COLUMN_NAME,
                    Bytes.toBytes(concurrentCounter), put)) {
                throw new TypeException("Concurrent create occurred for recordType <" + recordType.getName() + ">");
            }
            newRecordType.setId(uuid.toString());
            newRecordType.setVersion(recordTypeVersion);
            updateRecordTypeCache(newRecordType.clone(), null);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while creating recordType <" + recordType.getName()
                    + "> on HBase", e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while creating recordType <" + recordType.getName()
                    + "> on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while creating recordType <" + recordType.getName()
                    + "> on HBase", e);
        }
        return newRecordType;
    }

    public RecordType updateRecordType(RecordType recordType) throws RecordTypeNotFoundException,
            FieldTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(recordType, "recordType");
        RecordType newRecordType = recordType.clone();
        String id = newRecordType.getId();
        if (id == null) {
            throw new RecordTypeNotFoundException(newRecordType.getName(), null);
        }
        byte[] rowId = idToBytes(id);
        try {
            // Do an exists check first and avoid useless creation of the row
            // due to an incrementColumnValue call
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new RecordTypeNotFoundException(recordType.getId(), null);
            }
            // First increment the counter, then read the record type
            byte[] nameBytes = encodeName(recordType.getName());
            long concurrentCount = getTypeTable().incrementColumnValue(nameBytes, NON_VERSIONED_COLUMN_FAMILY,
                    CONCURRENT_COUNTER_COLUMN_NAME, 1);
            RecordType latestRecordType = getRecordTypeByIdFromHBase(id, null);
            Long latestRecordTypeVersion = latestRecordType.getVersion();
            Long newRecordTypeVersion = latestRecordTypeVersion + 1;

            Put put = new Put(rowId);
            boolean fieldTypeEntriesChanged = updateFieldTypeEntries(put, newRecordTypeVersion, newRecordType,
                    latestRecordType);

            boolean mixinsChanged = updateMixins(put, newRecordTypeVersion, newRecordType, latestRecordType);

            boolean nameChanged = updateName(put, recordType, latestRecordType);

            if (fieldTypeEntriesChanged || mixinsChanged || nameChanged) {
                put.add(NON_VERSIONED_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME, Bytes.toBytes(newRecordTypeVersion));
                getTypeTable().checkAndPut(nameBytes, NON_VERSIONED_COLUMN_FAMILY, CONCURRENT_COUNTER_COLUMN_NAME,
                        Bytes.toBytes(concurrentCount), put);
                newRecordType.setVersion(newRecordTypeVersion);
            } else {
                newRecordType.setVersion(latestRecordTypeVersion);
            }

            updateRecordTypeCache(newRecordType, latestRecordType);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while updating recordType <" + newRecordType.getId()
                    + "> on HBase", e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while updating recordType <" + newRecordType.getId()
                    + "> on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while updating recordType <" + newRecordType.getId()
                    + "> on HBase", e);
        }
        return newRecordType;
    }

    private boolean updateName(Put put, RecordType recordType, RecordType latestRecordType) throws TypeException {
        if (!recordType.getName().equals(latestRecordType.getName())) {
            try {
                getRecordTypeByName(recordType.getName(), null);
                throw new TypeException("Changing the name <" + recordType.getName() + "> of a recordType <"
                        + recordType.getId() + "> to a name that already exists is not allowed; old<"
                        + latestRecordType.getName() + "> new<" + recordType.getName() + ">");
            } catch (RecordTypeNotFoundException allowed) {
            }
            put.add(NON_VERSIONED_COLUMN_FAMILY, RECORDTYPE_NAME_COLUMN_NAME, encodeName(recordType.getName()));
            return true;
        }
        return false;
    }

    private RecordType getRecordTypeByIdFromHBase(String id, Long version) throws RecordTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(id, "recordTypeId");
        Get get = new Get(idToBytes(id));
        if (version != null) {
            get.setMaxVersions();
        }
        Result result;
        try {
            if (!getTypeTable().exists(get)) {
                throw new RecordTypeNotFoundException(id, null);
            }
            result = getTypeTable().get(get);
            // This covers the case where a given id would match a name that was
            // used for setting the concurrent counters
            if (result.getValue(NON_VERSIONED_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME) == null) {
                throw new RecordTypeNotFoundException(id, null);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving recordType <" + id + "> from HBase table", e);
        }
        NavigableMap<byte[], byte[]> nonVersionableColumnFamily = result.getFamilyMap(NON_VERSIONED_COLUMN_FAMILY);
        QName name;
        name = decodeName(nonVersionableColumnFamily.get(RECORDTYPE_NAME_COLUMN_NAME));
        RecordType recordType = newRecordType(id, name);
        Long currentVersion = Bytes.toLong(result.getValue(NON_VERSIONED_COLUMN_FAMILY, CURRENT_VERSION_COLUMN_NAME));
        if (version != null) {
            if (currentVersion < version) {
                throw new RecordTypeNotFoundException(id, version);
            }
            recordType.setVersion(version);
        } else {
            recordType.setVersion(currentVersion);
        }
        extractFieldTypeEntries(result, version, recordType);
        extractMixins(result, version, recordType);
        return recordType;
    }

    public RecordType getRecordTypeById(String id, Long version) throws RecordTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(id, "id");
        RecordType recordType = getRecordTypeFromCache(id);
        if (recordType == null) {
            throw new RecordTypeNotFoundException(id, version);
        }
        // The cache only keeps the latest (known) RecordType
        if (version != null && !version.equals(recordType.getVersion())) {
            recordType = getRecordTypeByIdFromHBase(id, version);
        }
        if (recordType == null) {
            throw new RecordTypeNotFoundException(id, version);
        }
        return recordType.clone();
    }
    
    public RecordType getRecordTypeByName(QName name, Long version) throws RecordTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(name, "name");
        RecordType recordType = getRecordTypeFromCache(name);
        if (recordType == null) {
            throw new RecordTypeNotFoundException(name, version);
        }
        // The cache only keeps the latest (known) RecordType
        if (version != null && !version.equals(recordType.getVersion())) {
            recordType = getRecordTypeByIdFromHBase(recordType.getId(), version);
        }
        if (recordType == null) {
            throw new RecordTypeNotFoundException(name, version);
        }
        return recordType.clone();
    }

    public synchronized Collection<RecordType> getRecordTypes() {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (RecordType recordType : recordTypeNameCache.values()) {
            recordTypes.add(recordType.clone());
        }
        return recordTypes;
    }

    private Long putMixinOnRecordType(Long recordTypeVersion, Put put, String mixinId, Long mixinVersion)
            throws RecordTypeNotFoundException, TypeException {
        Long newMixinVersion = getRecordTypeByIdFromHBase(mixinId, mixinVersion).getVersion();
        put.add(MIXIN_COLUMN_FAMILY, Bytes.toBytes(mixinId), recordTypeVersion, Bytes.toBytes(newMixinVersion));
        return newMixinVersion;
    }

    private boolean updateFieldTypeEntries(Put put, Long newRecordTypeVersion, RecordType recordType,
            RecordType latestRecordType) throws FieldTypeNotFoundException, TypeException {
        boolean changed = false;
        Collection<FieldTypeEntry> latestFieldTypeEntries = latestRecordType.getFieldTypeEntries();
        // Update FieldTypeEntries
        for (FieldTypeEntry fieldTypeEntry : recordType.getFieldTypeEntries()) {
            FieldTypeEntry latestFieldTypeEntry = latestRecordType.getFieldTypeEntry(fieldTypeEntry.getFieldTypeId());
            if (!fieldTypeEntry.equals(latestFieldTypeEntry)) {
                putFieldTypeEntry(newRecordTypeVersion, put, fieldTypeEntry);
                changed = true;
            }
            latestFieldTypeEntries.remove(latestFieldTypeEntry);
        }
        // Remove remaining FieldTypeEntries
        for (FieldTypeEntry fieldTypeEntry : latestFieldTypeEntries) {
            put.add(FIELDTYPEENTRY_COLUMN_FAMILY, idToBytes(fieldTypeEntry.getFieldTypeId()), newRecordTypeVersion,
                    new byte[] { EncodingUtil.DELETE_FLAG });
            changed = true;
        }
        return changed;
    }

    private void putFieldTypeEntry(Long version, Put put, FieldTypeEntry fieldTypeEntry)
            throws FieldTypeNotFoundException, TypeException {
        byte[] idBytes = idToBytes(fieldTypeEntry.getFieldTypeId());
        Get get = new Get(idBytes);
        try {
            if (!getTypeTable().exists(get)) {
                throw new FieldTypeNotFoundException(fieldTypeEntry.getFieldTypeId());
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while checking existance of FieldTypeEntry <"
                    + fieldTypeEntry.getFieldTypeId() + "> on HBase", e);
        }
        put.add(FIELDTYPEENTRY_COLUMN_FAMILY, idBytes, version, encodeFieldTypeEntry(fieldTypeEntry));
    }

    private boolean updateMixins(Put put, Long newRecordTypeVersion, RecordType recordType, RecordType latestRecordType) {
        boolean changed = false;
        Map<String, Long> latestMixins = latestRecordType.getMixins();
        // Update mixins
        for (Entry<String, Long> entry : recordType.getMixins().entrySet()) {
            String mixinId = entry.getKey();
            Long mixinVersion = entry.getValue();
            if (!mixinVersion.equals(latestMixins.get(mixinId))) {
                put.add(MIXIN_COLUMN_FAMILY, Bytes.toBytes(mixinId), newRecordTypeVersion, Bytes.toBytes(mixinVersion));
                changed = true;
            }
            latestMixins.remove(mixinId);
        }
        // Remove remaining mixins
        for (Entry<String, Long> entry : latestMixins.entrySet()) {
            put.add(MIXIN_COLUMN_FAMILY, Bytes.toBytes(entry.getKey()), newRecordTypeVersion,
                    new byte[] { EncodingUtil.DELETE_FLAG });
            changed = true;
        }
        return changed;
    }

    private void extractFieldTypeEntries(Result result, Long version, RecordType recordType) {
        if (version != null) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> fieldTypeEntriesVersionsMap = allVersionsMap
                    .get(FIELDTYPEENTRY_COLUMN_FAMILY);
            if (fieldTypeEntriesVersionsMap != null) {
                for (Entry<byte[], NavigableMap<Long, byte[]>> entry : fieldTypeEntriesVersionsMap.entrySet()) {
                    String fieldTypeId = idFromBytes(entry.getKey());
                    Entry<Long, byte[]> ceilingEntry = entry.getValue().ceilingEntry(version);
                    if (ceilingEntry != null) {
                        FieldTypeEntry fieldTypeEntry = decodeFieldTypeEntry(ceilingEntry.getValue(), fieldTypeId);
                        if (fieldTypeEntry != null) {
                            recordType.addFieldTypeEntry(fieldTypeEntry);
                        }
                    }
                }
            }
        } else {
            NavigableMap<byte[], byte[]> versionableMap = result.getFamilyMap(FIELDTYPEENTRY_COLUMN_FAMILY);
            if (versionableMap != null) {
                for (Entry<byte[], byte[]> entry : versionableMap.entrySet()) {
                    String fieldTypeId = idFromBytes(entry.getKey());
                    FieldTypeEntry fieldTypeEntry = decodeFieldTypeEntry(entry.getValue(), fieldTypeId);
                    if (fieldTypeEntry != null) {
                        recordType.addFieldTypeEntry(fieldTypeEntry);
                    }
                }
            }
        }
    }

    private void extractMixins(Result result, Long version, RecordType recordType) {
        if (version != null) {
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsMap = result.getMap();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> mixinVersionsMap = allVersionsMap.get(MIXIN_COLUMN_FAMILY);
            if (mixinVersionsMap != null) {
                for (Entry<byte[], NavigableMap<Long, byte[]>> entry : mixinVersionsMap.entrySet()) {
                    String mixinId = Bytes.toString(entry.getKey());
                    Entry<Long, byte[]> ceilingEntry = entry.getValue().ceilingEntry(version);
                    if (ceilingEntry != null) {
                        if (!EncodingUtil.isDeletedField(ceilingEntry.getValue())) {
                            recordType.addMixin(mixinId, Bytes.toLong(ceilingEntry.getValue()));
                        }
                    }
                }
            }
        } else {
            NavigableMap<byte[], byte[]> mixinMap = result.getFamilyMap(MIXIN_COLUMN_FAMILY);
            if (mixinMap != null) {
                for (Entry<byte[], byte[]> entry : mixinMap.entrySet()) {
                    if (!EncodingUtil.isDeletedField(entry.getValue())) {
                        recordType.addMixin(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
                    }
                }
            }
        }
    }

    /**
     * Encoding the fields: FD-version, mandatory, alias
     */
    private byte[] encodeFieldTypeEntry(FieldTypeEntry fieldTypeEntry) {
        byte[] bytes = new byte[0];
        bytes = Bytes.add(bytes, Bytes.toBytes(fieldTypeEntry.isMandatory()));
        return EncodingUtil.prefixValue(bytes, EncodingUtil.EXISTS_FLAG);
    }

    private FieldTypeEntry decodeFieldTypeEntry(byte[] bytes, String fieldTypeId) {
        if (EncodingUtil.isDeletedField(bytes)) {
            return null;
        }
        byte[] encodedBytes = EncodingUtil.stripPrefix(bytes);
        boolean mandatory = Bytes.toBoolean(encodedBytes);
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }

    

    public FieldType createFieldType(FieldType fieldType) throws FieldTypeExistsException, TypeException {
        ArgumentValidator.notNull(fieldType, "fieldType");

        FieldType newFieldType;
        Long version = Long.valueOf(1);
        try {
            UUID uuid = getValidUUID();
            byte[] rowId = idToBytes(uuid);
            // Take a counter on a row with the name as key
            byte[] nameBytes = encodeName(fieldType.getName());
            long concurrentCounter = getTypeTable().incrementColumnValue(nameBytes, NON_VERSIONED_COLUMN_FAMILY,
                    CONCURRENT_COUNTER_COLUMN_NAME, 1);

            if (getFieldTypeFromCache(fieldType.getName()) != null)
                throw new FieldTypeExistsException(fieldType);

            Put put = new Put(rowId);
            put.add(NON_VERSIONED_COLUMN_FAMILY, FIELDTYPE_VALUETYPE_COLUMN_NAME, fieldType.getValueType().toBytes());
            put.add(NON_VERSIONED_COLUMN_FAMILY, FIELDTPYE_SCOPE_COLUMN_NAME, Bytes
                    .toBytes(fieldType.getScope().name()));
            put.add(NON_VERSIONED_COLUMN_FAMILY, FIELDTYPE_NAME_COLUMN_NAME, nameBytes);
            if (!getTypeTable().checkAndPut(nameBytes, NON_VERSIONED_COLUMN_FAMILY, CONCURRENT_COUNTER_COLUMN_NAME,
                    Bytes.toBytes(concurrentCounter), put)) {
                throw new TypeException("Concurrent create occurred for fieldType <" + fieldType.getName() + ">");
            }
            newFieldType = fieldType.clone();
            newFieldType.setId(uuid.toString());
            updateFieldTypeCache(newFieldType, null);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while creating fieldType <" + fieldType.getName()
                    + "> version: <" + version + "> on HBase", e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while creating fieldType <" + fieldType.getName()
                    + "> version: <" + version + "> on HBase", e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while creating fieldType <" + fieldType.getName()
                    + "> version: <" + version + "> on HBase", e);
        }
        return newFieldType;
    }

    public FieldType updateFieldType(FieldType fieldType) throws FieldTypeNotFoundException, FieldTypeUpdateException,
            TypeException {
        byte[] rowId = idToBytes(fieldType.getId());
        try {
            // Do an exists check first and avoid useless creation of the row
            // due to an incrementColumnValue call
            if (!getTypeTable().exists(new Get(rowId))) {
                throw new FieldTypeNotFoundException(fieldType.getId());
            }
            // First increment the counter on the row with the name as key, then
            // read the field type
            byte[] nameBytes = encodeName(fieldType.getName());
            long concurrentCounter = getTypeTable().incrementColumnValue(nameBytes, NON_VERSIONED_COLUMN_FAMILY,
                    CONCURRENT_COUNTER_COLUMN_NAME, 1);
            FieldType latestFieldType = getFieldTypeByIdFromHBase(fieldType.getId());
            if (!fieldType.getValueType().equals(latestFieldType.getValueType())) {
                throw new FieldTypeUpdateException("Changing the valueType of a fieldType <" + fieldType.getId()
                        + "> is not allowed; old<" + latestFieldType.getValueType() + "> new<"
                        + fieldType.getValueType() + ">");
            }
            if (!fieldType.getScope().equals(latestFieldType.getScope())) {
                throw new FieldTypeUpdateException("Changing the scope of a fieldType <" + fieldType.getId()
                        + "> is not allowed; old<" + latestFieldType.getScope() + "> new<" + fieldType.getScope() + ">");
            }
            if (!fieldType.getName().equals(latestFieldType.getName())) {
                try {
                    getFieldTypeByName(fieldType.getName());
                    throw new FieldTypeUpdateException("Changing the name <" + fieldType.getName()
                            + "> of a fieldType <" + fieldType.getId()
                            + "> to a name that already exists is not allowed; old<" + latestFieldType.getName()
                            + "> new<" + fieldType.getName() + ">");
                } catch (FieldTypeNotFoundException allowed) {
                }
                Put put = new Put(rowId);
                put.add(NON_VERSIONED_COLUMN_FAMILY, FIELDTYPE_NAME_COLUMN_NAME, nameBytes);
                getTypeTable().checkAndPut(nameBytes, NON_VERSIONED_COLUMN_FAMILY, CONCURRENT_COUNTER_COLUMN_NAME,
                        Bytes.toBytes(concurrentCounter), put);
            }
            updateFieldTypeCache(fieldType, latestFieldType);
        } catch (IOException e) {
            throw new TypeException("Exception occurred while updating fieldType <" + fieldType.getId() + "> on HBase",
                    e);
        } catch (KeeperException e) {
            throw new TypeException("Exception occurred while updating fieldType <" + fieldType.getId() + "> on HBase",
                    e);
        } catch (InterruptedException e) {
            throw new TypeException("Exception occurred while updating fieldType <" + fieldType.getId() + "> on HBase",
                    e);
        }
        return fieldType.clone();
    }

    
    private FieldType getFieldTypeByIdFromHBase(String id) throws FieldTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(id, "id");
        Result result;
        Get get = new Get(idToBytes(id));
        try {
            if (!getTypeTable().exists(get)) {
                throw new FieldTypeNotFoundException(id);
            }
            result = getTypeTable().get(get);
            // This covers the case where a given id would match a name that was
            // used for setting the concurrent counters
            if (result.getValue(NON_VERSIONED_COLUMN_FAMILY, FIELDTYPE_NAME_COLUMN_NAME) == null) {
                throw new FieldTypeNotFoundException(id);
            }
        } catch (IOException e) {
            throw new TypeException("Exception occurred while retrieving fieldType <" + id + "> from HBase", e);
        }
        NavigableMap<byte[], byte[]> nonVersionableColumnFamily = result.getFamilyMap(NON_VERSIONED_COLUMN_FAMILY);
        QName name;
        name = decodeName(nonVersionableColumnFamily.get(FIELDTYPE_NAME_COLUMN_NAME));
        ValueType valueType = ValueTypeImpl.fromBytes(nonVersionableColumnFamily.get(FIELDTYPE_VALUETYPE_COLUMN_NAME),
                this);
        Scope scope = Scope.valueOf(Bytes.toString(nonVersionableColumnFamily.get(FIELDTPYE_SCOPE_COLUMN_NAME)));
        return new FieldTypeImpl(id, valueType, name, scope);
    }

    public FieldType getFieldTypeById(String id) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(id, "id");
        FieldType fieldType = getFieldTypeFromCache(id);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(id);
        }
        return fieldType.clone();
    }

    public FieldType getFieldTypeByName(QName name) throws FieldTypeNotFoundException {
        ArgumentValidator.notNull(name, "name");
        FieldType fieldType = getFieldTypeFromCache(name);
        if (fieldType == null) {
            throw new FieldTypeNotFoundException(name);
        }
        return fieldType.clone();
    }

    public synchronized List<FieldType> getFieldTypes() {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldType fieldType : fieldTypeNameCache.values()) {
            fieldTypes.add(fieldType.clone());
        }
        return fieldTypes;
    }

    private synchronized FieldType getFieldTypeFromCache(QName name) {
        return fieldTypeNameCache.get(name);
    }
    
    private synchronized FieldType getFieldTypeFromCache(String id) {
        return fieldTypeIdCache.get(id);
    }
    
    private synchronized RecordType getRecordTypeFromCache(QName name) {
        return recordTypeNameCache.get(name);
    }

    private synchronized RecordType getRecordTypeFromCache(String id) {
        return recordTypeIdCache.get(id);
    }
    
    private synchronized void initializeCaches() throws InterruptedException {
        try {
            zooKeeper.getData(CACHE_INVALIDATION_PATH, cacheWatcher, null);
        } catch (KeeperException e) {
            // Failed to put our watcher.
            // Relying on the ConnectionWatcher to put it again and initialize
            // the caches.
        }
        initializeFieldTypeCache();
        initializeRecordTypeCache();
    }

    private synchronized void initializeFieldTypeCache() {
        Map<QName, FieldType> newFieldTypeNameCache = new HashMap<QName, FieldType>();
        Map<String, FieldType> newFieldTypeIdCache = new HashMap<String, FieldType>();
        ResultScanner scanner = null;
        try {
            scanner = getTypeTable().getScanner(NON_VERSIONED_COLUMN_FAMILY, FIELDTYPE_NAME_COLUMN_NAME);
            for (Result result : scanner) {
                String id = idFromBytes(result.getRow());
                FieldType fieldType = getFieldTypeByIdFromHBase(id);
                QName name = decodeName(result.getValue(NON_VERSIONED_COLUMN_FAMILY, FIELDTYPE_NAME_COLUMN_NAME));
                newFieldTypeNameCache.put(name, fieldType);
                newFieldTypeIdCache.put(id, fieldType);
            }
            fieldTypeNameCache = newFieldTypeNameCache;
            fieldTypeIdCache = newFieldTypeIdCache;
        } catch (Exception e) {
            // We keep on working with the old cache
            log.warn("Exception while initializing FieldType cache. Cache is possibly out of date.", e);
        } finally {
            Closer.close(scanner);
        }
    }

    private synchronized void initializeRecordTypeCache() {
        Map<QName, RecordType> newRecordTypeNameCache = new HashMap<QName, RecordType>();
        Map<String, RecordType> newRecordTypeIdCache = new HashMap<String, RecordType>();
        ResultScanner scanner = null;
        try {
            scanner = getTypeTable().getScanner(NON_VERSIONED_COLUMN_FAMILY, RECORDTYPE_NAME_COLUMN_NAME);
            for (Result result : scanner) {
                String id = idFromBytes(result.getRow());
                RecordType recordType = getRecordTypeByIdFromHBase(id, null);
                QName name = decodeName(result.getValue(NON_VERSIONED_COLUMN_FAMILY, RECORDTYPE_NAME_COLUMN_NAME));
                newRecordTypeNameCache.put(name, recordType);
                newRecordTypeIdCache.put(id, recordType);
            }
            recordTypeNameCache = newRecordTypeNameCache;
            recordTypeIdCache = newRecordTypeIdCache;
        } catch (Exception e) {
            // We keep on working with the old cache
            log.warn("Exception while initializing RecordType cache. Cache is possibly out of date.", e);
        } finally {
            Closer.close(scanner);
        }
    }

    // FieldType name cache
    private synchronized void updateFieldTypeCache(FieldType fieldType, FieldType oldFieldType) throws KeeperException,
            InterruptedException {
        if (oldFieldType != null) {
            fieldTypeNameCache.remove(oldFieldType.getName());
            fieldTypeIdCache.remove(oldFieldType.getId());
        }
        fieldTypeNameCache.put(fieldType.getName(), fieldType);
        fieldTypeIdCache.put(fieldType.getId(), fieldType);
        notifyCacheInvalidate();
    }

    // RecordType name cache
    private synchronized void updateRecordTypeCache(RecordType recordType, RecordType oldType) throws KeeperException,
            InterruptedException {
        if (oldType != null) {
            recordTypeNameCache.remove(oldType.getName());
            recordTypeIdCache.remove(oldType.getId());
        }
        recordTypeNameCache.put(recordType.getName(), recordType);
        recordTypeIdCache.put(recordType.getId(), recordType);
        notifyCacheInvalidate();
    }

    private void notifyCacheInvalidate() throws KeeperException, InterruptedException {
        zooKeeper.setData(CACHE_INVALIDATION_PATH, null, -1);
    }

    private byte[] encodeName(QName qname) {
        byte[] encodedName = new byte[0];
        String name = qname.getName();
        String namespace = qname.getNamespace();

        if (namespace == null) {
            encodedName = Bytes.add(encodedName, Bytes.toBytes(0));
        } else {
            encodedName = Bytes.add(encodedName, Bytes.toBytes(namespace.length()));
            encodedName = Bytes.add(encodedName, Bytes.toBytes(namespace));
        }
        encodedName = Bytes.add(encodedName, Bytes.toBytes(name.length()));
        encodedName = Bytes.add(encodedName, Bytes.toBytes(name));
        return encodedName;
    }

    private QName decodeName(byte[] bytes) {
        int offset = 0;
        String namespace = null;
        int namespaceLength = Bytes.toInt(bytes);
        offset = offset + Bytes.SIZEOF_INT;
        if (namespaceLength > 0) {
            namespace = Bytes.toString(bytes, offset, namespaceLength);
        }
        offset = offset + namespaceLength;
        int nameLength = Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
        offset = offset + Bytes.SIZEOF_INT;
        String name = Bytes.toString(bytes, offset, nameLength);
        return new QName(namespace, name);
    }

    private byte[] idToBytes(UUID id) {
        byte[] rowId;
        rowId = new byte[16];
        Bytes.putLong(rowId, 0, id.getMostSignificantBits());
        Bytes.putLong(rowId, 8, id.getLeastSignificantBits());
        return rowId;
    }

    private byte[] idToBytes(String id) {
        UUID uuid = UUID.fromString(id);
        byte[] rowId;
        rowId = new byte[16];
        Bytes.putLong(rowId, 0, uuid.getMostSignificantBits());
        Bytes.putLong(rowId, 8, uuid.getLeastSignificantBits());
        return rowId;
    }

    private String idFromBytes(byte[] bytes) {
        UUID uuid = new UUID(Bytes.toLong(bytes, 0, 8), Bytes.toLong(bytes, 8, 8));
        return uuid.toString();
    }

    /**
     * Generates a uuid and checks if it's not already in use
     */
    private UUID getValidUUID() throws IOException {
        UUID uuid = UUID.randomUUID();
        byte[] rowId = idToBytes(uuid);
        // The chance it would already exist is small
        if (typeTable.exists(new Get(rowId)))
            return getValidUUID();
        // The chance a same uuid is generated after doing the exists check is
        // even smaller
        // If it would still happen, the incrementColumnValue would return a
        // number bigger than 1
        if (1L != typeTable
                .incrementColumnValue(rowId, NON_VERSIONED_COLUMN_FAMILY, CONCURRENT_COUNTER_COLUMN_NAME, 1L)) {
            return getValidUUID();
        }
        return uuid;
    }

    protected HTableInterface getTypeTable() {
        return typeTable;
    }

}

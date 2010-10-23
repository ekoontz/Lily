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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.Map.Entry;

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
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.FieldTypeExistsException;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.FieldTypeUpdateException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeExistsException;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.hbase.LocalHTable;
import org.lilyproject.util.io.Closer;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public class HBaseTypeManager extends AbstractTypeManager implements TypeManager {
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

    private HTableInterface typeTable;

    public HBaseTypeManager(IdGenerator idGenerator, Configuration configuration, ZooKeeperItf zooKeeper)
            throws IOException, InterruptedException, KeeperException {
        super(zooKeeper);
        log = LogFactory.getLog(getClass());
        this.idGenerator = idGenerator;

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
        setupCaches();
    }
    
    protected void cacheInvalidationReconnected() throws InterruptedException {
        super.cacheInvalidationReconnected();
        try {
            // A previous notify might have failed because of a disconnection
            // To be sure we try to send a notify again
            notifyCacheInvalidate();
        } catch (KeeperException e) {
            log.info("Exception occured while sending a cache invalidation notification after reconnecting to zookeeper");
        }
    }
    
    // Only invalidate the cache from the server-side
    // Updates from the RemoteTypeManager will have to pass through (server-side) HBaseTypeManager anyway
    private void notifyCacheInvalidate() throws KeeperException, InterruptedException {
        zooKeeper.setData(CACHE_INVALIDATION_PATH, null, -1);
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
            updateRecordTypeCache(newRecordType.clone());
            notifyCacheInvalidate();
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

    private Long putMixinOnRecordType(Long recordTypeVersion, Put put, String mixinId, Long mixinVersion)
            throws RecordTypeNotFoundException, TypeException {
        Long newMixinVersion = getRecordTypeByIdWithoutCache(mixinId, mixinVersion).getVersion();
        put.add(MIXIN_COLUMN_FAMILY, Bytes.toBytes(mixinId), recordTypeVersion, Bytes.toBytes(newMixinVersion));
        return newMixinVersion;
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
            RecordType latestRecordType = getRecordTypeByIdWithoutCache(id, null);
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

            updateRecordTypeCache(newRecordType);
            notifyCacheInvalidate();
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

    protected RecordType getRecordTypeByIdWithoutCache(String id, Long version) throws RecordTypeNotFoundException,
            TypeException {
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
            updateFieldTypeCache(newFieldType);
            notifyCacheInvalidate();
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
            FieldType latestFieldType = getFieldTypeByIdWithoutCache(fieldType.getId());
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
            updateFieldTypeCache(fieldType);
            notifyCacheInvalidate();
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
    
    private FieldType getFieldTypeByIdWithoutCache(String id) throws FieldTypeNotFoundException, TypeException {
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

    protected List<FieldType> getFieldTypesWithoutCache() throws IOException, FieldTypeNotFoundException,
            TypeException {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        ResultScanner scanner = null;
        try {
            scanner = getTypeTable().getScanner(NON_VERSIONED_COLUMN_FAMILY, FIELDTYPE_NAME_COLUMN_NAME);
            for (Result result : scanner) {
                String id = idFromBytes(result.getRow());
                FieldType fieldType = getFieldTypeByIdWithoutCache(id);
                fieldTypes.add(fieldType);
            }
        } finally {
            Closer.close(scanner);
        }
        return fieldTypes;
    }

    protected List<RecordType> getRecordTypesWithoutCache() throws IOException, RecordTypeNotFoundException,
            TypeException {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        ResultScanner scanner = null;
        try {
            scanner = getTypeTable().getScanner(NON_VERSIONED_COLUMN_FAMILY, RECORDTYPE_NAME_COLUMN_NAME);
            for (Result result : scanner) {
                String id = idFromBytes(result.getRow());
                RecordType recordType = getRecordTypeByIdWithoutCache(id, null);
                recordTypes.add(recordType);
            }
        } finally {
            Closer.close(scanner);
        }
        return recordTypes;
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

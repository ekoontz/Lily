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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeEntry;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.PrimitiveValueType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordType;
import org.lilyproject.repository.api.RecordTypeNotFoundException;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.primitivevaluetype.BlobValueType;
import org.lilyproject.repository.impl.primitivevaluetype.BooleanValueType;
import org.lilyproject.repository.impl.primitivevaluetype.DateTimeValueType;
import org.lilyproject.repository.impl.primitivevaluetype.DateValueType;
import org.lilyproject.repository.impl.primitivevaluetype.DecimalValueType;
import org.lilyproject.repository.impl.primitivevaluetype.DoubleValueType;
import org.lilyproject.repository.impl.primitivevaluetype.IntegerValueType;
import org.lilyproject.repository.impl.primitivevaluetype.LinkValueType;
import org.lilyproject.repository.impl.primitivevaluetype.LongValueType;
import org.lilyproject.repository.impl.primitivevaluetype.StringValueType;
import org.lilyproject.repository.impl.primitivevaluetype.UriValueType;
import org.lilyproject.util.ArgumentValidator;
import org.lilyproject.util.Logs;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

public abstract class AbstractTypeManager implements TypeManager {
    protected Log log;

    protected CacheRefresher cacheRefresher = new CacheRefresher();
    
    protected Map<String, PrimitiveValueType> primitiveValueTypes = new HashMap<String, PrimitiveValueType>();
    protected IdGenerator idGenerator;
    
    //
    // Caching
    //
    protected ZooKeeperItf zooKeeper;
    private Map<QName, FieldType> fieldTypeNameCache = new HashMap<QName, FieldType>();
    private Map<QName, RecordType> recordTypeNameCache = new HashMap<QName, RecordType>();
    private Map<String, FieldType> fieldTypeIdCache = new HashMap<String, FieldType>();
    private Map<String, RecordType> recordTypeIdCache = new HashMap<String, RecordType>();
    private final CacheWatcher cacheWatcher = new CacheWatcher();
    protected static final String CACHE_INVALIDATION_PATH = "/lily/typemanager/cache";
    
    public AbstractTypeManager(ZooKeeperItf zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
    
    public void close() throws IOException {
        try {
            cacheRefresher.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted", e);
        }
    }
    
    private class CacheWatcher implements Watcher {
        public void process(WatchedEvent event) {
            cacheRefresher.needsRefresh();
        }
    }

    protected class ConnectionWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (EventType.None.equals(event.getType()) && KeeperState.SyncConnected.equals(event.getState())) {
                try {
                    cacheInvalidationReconnected();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                cacheRefresher.needsRefresh();
            }
        }
    }

    private class CacheRefresher implements Runnable {
        private volatile boolean needsRefresh;
        private volatile boolean stop;
        private final Object needsRefreshLock = new Object();
        private Thread thread;

        public void start() {
            thread = new Thread(this, "TypeManager cache refresher");
            thread.setDaemon(true); // Since this might be used in clients 
            thread.start();
        }

        public void stop() throws InterruptedException {
            stop = true;
            if (thread != null) {
                thread.interrupt();
                Logs.logThreadJoin(thread);
                thread.join();
                thread = null;
            }
        }

        public void needsRefresh() {
            synchronized (needsRefreshLock) {
                needsRefresh = true;
                needsRefreshLock.notifyAll();
            }
        }

        public void run() {
            while (!stop && !Thread.interrupted()) {
                try {
                    if (needsRefresh) {
                        synchronized (needsRefreshLock) {
                            needsRefresh = false;
                        }
                        refreshCaches();
                    }

                    synchronized (needsRefreshLock) {
                        if (!needsRefresh && !stop) {
                            needsRefreshLock.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error refreshing type manager cache.", t);
                }
            }
        }
    }

    protected void cacheInvalidationReconnected() throws InterruptedException {
    }

    protected void setupCaches() throws InterruptedException, KeeperException {
        ZkUtil.createPath(zooKeeper, CACHE_INVALIDATION_PATH);
        zooKeeper.addDefaultWatcher(new ConnectionWatcher());
        refreshCaches();
    }

    private synchronized void refreshCaches() throws InterruptedException {
        try {
            zooKeeper.getData(CACHE_INVALIDATION_PATH, cacheWatcher, null);
        } catch (KeeperException e) {
            // Failed to put our watcher.
            // Relying on the ConnectionWatcher to put it again and initialize
            // the caches.
        }
        refreshFieldTypeCache();
        refreshRecordTypeCache();
    }

    private synchronized void refreshFieldTypeCache() {
        Map<QName, FieldType> newFieldTypeNameCache = new HashMap<QName, FieldType>();
        Map<String, FieldType> newFieldTypeIdCache = new HashMap<String, FieldType>();
        try {
            List<FieldType> fieldTypes = getFieldTypesWithoutCache();
            for (FieldType fieldType : fieldTypes) {
                newFieldTypeNameCache.put(fieldType.getName(), fieldType);
                newFieldTypeIdCache.put(fieldType.getId(), fieldType);
            }
            fieldTypeNameCache = newFieldTypeNameCache;
            fieldTypeIdCache = newFieldTypeIdCache;
        } catch (Exception e) {
            // We keep on working with the old cache
            log.warn("Exception while refreshing FieldType cache. Cache is possibly out of date.", e);
        }
    }
    
    private synchronized void refreshRecordTypeCache() {
        Map<QName, RecordType> newRecordTypeNameCache = new HashMap<QName, RecordType>();
        Map<String, RecordType> newRecordTypeIdCache = new HashMap<String, RecordType>();
        try {
            List<RecordType> recordTypes = getRecordTypesWithoutCache();
            for (RecordType recordType : recordTypes) {
                newRecordTypeNameCache.put(recordType.getName(), recordType);
                newRecordTypeIdCache.put(recordType.getId(), recordType);
            }
            recordTypeNameCache = newRecordTypeNameCache;
            recordTypeIdCache = newRecordTypeIdCache;
        } catch (Exception e) {
            // We keep on working with the old cache
            log.warn("Exception while refreshing RecordType cache. Cache is possibly out of date.", e);
        } 
    }

    abstract protected List<FieldType> getFieldTypesWithoutCache() throws IOException, FieldTypeNotFoundException, TypeException;
    abstract protected List<RecordType> getRecordTypesWithoutCache() throws IOException, RecordTypeNotFoundException, TypeException;
    
    
    protected synchronized void updateFieldTypeCache(FieldType fieldType) {
        FieldType oldFieldType = getFieldTypeFromCache(fieldType.getId());
        if (oldFieldType != null) {
            fieldTypeNameCache.remove(oldFieldType.getName());
            fieldTypeIdCache.remove(oldFieldType.getId());
        }
        fieldTypeNameCache.put(fieldType.getName(), fieldType);
        fieldTypeIdCache.put(fieldType.getId(), fieldType);
    }

    protected synchronized void updateRecordTypeCache(RecordType recordType) {
        RecordType oldType = getRecordTypeFromCache(recordType.getId());
        if (oldType != null) {
            recordTypeNameCache.remove(oldType.getName());
            recordTypeIdCache.remove(oldType.getId());
        }
        recordTypeNameCache.put(recordType.getName(), recordType);
        recordTypeIdCache.put(recordType.getId(), recordType);
    }
    
    public synchronized Collection<RecordType> getRecordTypes() {
        List<RecordType> recordTypes = new ArrayList<RecordType>();
        for (RecordType recordType : recordTypeNameCache.values()) {
            recordTypes.add(recordType.clone());
        }
        return recordTypes;
    }
    
    public synchronized List<FieldType> getFieldTypes() {
        List<FieldType> fieldTypes = new ArrayList<FieldType>();
        for (FieldType fieldType : fieldTypeNameCache.values()) {
            fieldTypes.add(fieldType.clone());
        }
        return fieldTypes;
    }

    protected synchronized FieldType getFieldTypeFromCache(QName name) {
        return fieldTypeNameCache.get(name);
    }
    
    protected synchronized FieldType getFieldTypeFromCache(String id) {
        return fieldTypeIdCache.get(id);
    }
    
    protected synchronized RecordType getRecordTypeFromCache(QName name) {
        return recordTypeNameCache.get(name);
    }

    protected synchronized RecordType getRecordTypeFromCache(String id) {
        return recordTypeIdCache.get(id);
    }
    
    public RecordType getRecordTypeById(String id, Long version) throws RecordTypeNotFoundException, TypeException {
        ArgumentValidator.notNull(id, "id");
        RecordType recordType = getRecordTypeFromCache(id);
        if (recordType == null) {
            throw new RecordTypeNotFoundException(id, version);
        }
        // The cache only keeps the latest (known) RecordType
        if (version != null && !version.equals(recordType.getVersion())) {
            recordType = getRecordTypeByIdWithoutCache(id, version);
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
            recordType = getRecordTypeByIdWithoutCache(recordType.getId(), version);
        }
        if (recordType == null) {
            throw new RecordTypeNotFoundException(name, version);
        }
        return recordType.clone();
    }
    
    abstract protected RecordType getRecordTypeByIdWithoutCache(String id, Long version) throws RecordTypeNotFoundException, TypeException;
    
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
    
    //
    // Object creation methods
    //
    public RecordType newRecordType(QName name) {
        return new RecordTypeImpl(null, name);
    }
    
    public RecordType newRecordType(String recordTypeId, QName name) {
        ArgumentValidator.notNull(name, "name");
        return new RecordTypeImpl(recordTypeId, name);
    }

    public FieldType newFieldType(ValueType valueType, QName name, Scope scope) {
        return newFieldType(null, valueType, name, scope);
    }

    public FieldTypeEntry newFieldTypeEntry(String fieldTypeId, boolean mandatory) {
        ArgumentValidator.notNull(fieldTypeId, "fieldTypeId");
        ArgumentValidator.notNull(mandatory, "mandatory");
        return new FieldTypeEntryImpl(fieldTypeId, mandatory);
    }


    public FieldType newFieldType(String id, ValueType valueType, QName name,
            Scope scope) {
                ArgumentValidator.notNull(valueType, "valueType");
                ArgumentValidator.notNull(name, "name");
                ArgumentValidator.notNull(scope, "scope");
                return new FieldTypeImpl(id, valueType, name, scope);
            }

    //
    // Primitive value types
    //
    public void registerPrimitiveValueType(PrimitiveValueType primitiveValueType) {
        primitiveValueTypes.put(primitiveValueType.getName(), primitiveValueType);
    }

    public ValueType getValueType(String primitiveValueTypeName, boolean multivalue, boolean hierarchy) {
        PrimitiveValueType type = primitiveValueTypes.get(primitiveValueTypeName);
        if (type == null) {
            throw new IllegalArgumentException("Primitive value type does not exist: " + primitiveValueTypeName);
        }
        return new ValueTypeImpl(type, multivalue, hierarchy);
    }
    
    // TODO get this from some configuration file
    protected void registerDefaultValueTypes() {
        //
        // Important:
        //
        // When adding a type below, please update the list of built-in
        // types in the javadoc of the method TypeManager.getValueType.
        //

        registerPrimitiveValueType(new StringValueType());
        registerPrimitiveValueType(new IntegerValueType());
        registerPrimitiveValueType(new LongValueType());
        registerPrimitiveValueType(new DoubleValueType());
        registerPrimitiveValueType(new DecimalValueType());
        registerPrimitiveValueType(new BooleanValueType());
        registerPrimitiveValueType(new DateValueType());
        registerPrimitiveValueType(new DateTimeValueType());
        registerPrimitiveValueType(new LinkValueType(idGenerator));
        registerPrimitiveValueType(new BlobValueType());
        registerPrimitiveValueType(new UriValueType());
    }
}

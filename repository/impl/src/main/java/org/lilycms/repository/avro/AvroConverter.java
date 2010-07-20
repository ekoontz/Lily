package org.lilycms.repository.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.lilycms.repository.api.*;

public class AvroConverter {

    private TypeManager typeManager;
    private Repository repository;

    public AvroConverter() {
    }
    
    public void setRepository(Repository repository) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
    }
    
    public Record convert(AvroRecord avroRecord) {
        Record record = repository.newRecord();
        // Id
        if (avroRecord.id != null) {
            record.setId(repository.getIdGenerator().fromString(avroRecord.id.toString()));
        }
        if (avroRecord.version != null) {
            record.setVersion(avroRecord.version);
        }
        // Record Types
        if (avroRecord.recordTypeId != null) {
            record.setRecordType(avroRecord.recordTypeId.toString(), avroRecord.version);
        }
        
        Map<Utf8, Utf8> scopeRecordTypeIds = avroRecord.scopeRecordTypeIds;
        if (scopeRecordTypeIds != null) {
            for (Scope scope : Scope.values()) {
                Utf8 key = new Utf8(scope.name());
                Utf8 recordTypeId = scopeRecordTypeIds.get(key);
                if (recordTypeId != null) {
                    record.setRecordType(scope, recordTypeId.toString(), avroRecord.scopeRecordTypeVersions.get(key));
                }
            }
        }

        // Fields
        if (avroRecord.fields != null) {
            for (AvroField field : avroRecord.fields) {
                QName name = decodeQName(convert(field.name));
                ValueType valueType = typeManager.getValueType(convert(field.primitiveType), field.multiValue, field.hierarchical);
                Object value = valueType.fromBytes(field.value.array());
                record.setField(name, value);
            }
        }

        // FieldsToDelete
        GenericArray<Utf8> avroFieldsToDelete = avroRecord.fieldsToDelete;
        if (avroFieldsToDelete != null) {
            List<QName> fieldsToDelete = new ArrayList<QName>();
            for (Utf8 fieldToDelete : avroFieldsToDelete) {
                fieldsToDelete.add(decodeQName(convert(fieldToDelete)));
            }
            record.addFieldsToDelete(fieldsToDelete);
        }
        return record;
    }
    
    public AvroRecord convert(Record record) throws AvroFieldTypeNotFoundException, AvroTypeException {
        AvroRecord avroRecord = new AvroRecord();
        // Id
        RecordId id = record.getId();
        if (id != null) {
            avroRecord.id = new Utf8(id.toString());
        }
        if (record.getVersion() != null) {
            avroRecord.version = record.getVersion();
        } else { avroRecord.version = null; }
        // Record types
        if (record.getRecordTypeId() != null) {
            avroRecord.recordTypeId = new Utf8(record.getRecordTypeId());
        } else { avroRecord.recordTypeId = null;}
        if (record.getRecordTypeVersion() != null) {
            avroRecord.recordTypeVersion = record.getRecordTypeVersion();
        }
        avroRecord.scopeRecordTypeIds = new HashMap<Utf8, Utf8>();
        avroRecord.scopeRecordTypeVersions = new HashMap<Utf8, Long>();
        for (Scope scope : Scope.values()) {
            String recordTypeId = record.getRecordTypeId(scope);
            if (recordTypeId != null) {
                avroRecord.scopeRecordTypeIds.put(new Utf8(scope.name()), new Utf8(recordTypeId));
                Long version = record.getRecordTypeVersion(scope);
                if (version != null) {
                    avroRecord.scopeRecordTypeVersions.put(new Utf8(scope.name()), version);
                }
            }
        }

        // Fields
        avroRecord.fields = new GenericData.Array<AvroField>(record.getFields().size(), Schema.createArray(AvroField.SCHEMA$));
        for (Entry<QName, Object> field : record.getFields().entrySet()) {
            AvroField avroField = new AvroField();
            avroField.name = convert(encodeQName(field.getKey()));

            FieldType fieldType;
            try {
                fieldType = typeManager.getFieldTypeByName(field.getKey());
            } catch (FieldTypeNotFoundException e) {
                throw convert(e);
            } catch (TypeException e) {
                throw convert(e);
            }

            avroField.primitiveType = convert(fieldType.getValueType().getPrimitive().getName());
            avroField.multiValue = fieldType.getValueType().isMultiValue();
            avroField.hierarchical = fieldType.getValueType().isHierarchical();

            byte[] value = fieldType.getValueType().toBytes(field.getValue());
            ByteBuffer byteBuffer = ByteBuffer.allocate(value.length);
            byteBuffer.mark();
            byteBuffer.put(value);
            byteBuffer.reset();

            avroField.value = byteBuffer;

            avroRecord.fields.add(avroField);
        }

        // FieldsToDelete
        List<QName> fieldsToDelete = record.getFieldsToDelete();
        avroRecord.fieldsToDelete = new GenericData.Array<Utf8>(fieldsToDelete.size(), Schema.createArray(Schema.create(Schema.Type.STRING)));
        for (QName fieldToDelete : fieldsToDelete) {
            avroRecord.fieldsToDelete.add(new Utf8(encodeQName(fieldToDelete)));
        }
        return avroRecord; 
    }

    // The key of a map can only be a string in avro
    private String encodeQName(QName qname) {
        StringBuilder stringBuilder = new StringBuilder();
        String namespace = qname.getNamespace();
        if (namespace != null) {
            stringBuilder.append(namespace);
        }
        stringBuilder.append(":");
        stringBuilder.append(qname.getName());
        return stringBuilder.toString();
    }

    // The key of a map can only be a string in avro
    private QName decodeQName(String string) {
        int separatorIndex = string.indexOf(":");
        String namespace = null;
        if (separatorIndex != 0) {
            namespace = string.substring(0, separatorIndex);
        }
        String name = string.substring(separatorIndex+1);
        return new QName(namespace, name);
    }
    
    
    public FieldType convert(AvroFieldType avroFieldType) {
        ValueType valueType = convert(avroFieldType.valueType);
        QName name = convert(avroFieldType.name);
        String id = convert(avroFieldType.id);
        if (id != null) {
            return typeManager.newFieldType(id, valueType, name, avroFieldType.scope);
        }
        return typeManager.newFieldType(valueType, name, avroFieldType.scope);
    }

    public AvroFieldType convert(FieldType fieldType) {
        AvroFieldType avroFieldType = new AvroFieldType();
        
        if (fieldType.getId() != null) {
            avroFieldType.id = new Utf8(fieldType.getId());
        } 
        avroFieldType.name = convert(fieldType.getName());
        avroFieldType.valueType = convert(fieldType.getValueType());
        avroFieldType.scope = fieldType.getScope();
        return avroFieldType;
    }

    public RecordType convert(AvroRecordType avroRecordType) {
        String recordTypeId = convert(avroRecordType.id);
        RecordType recordType = typeManager.newRecordType(recordTypeId);
        recordType.setVersion(avroRecordType.version);
        GenericArray<AvroFieldTypeEntry> fieldTypeEntries = avroRecordType.fieldTypeEntries;
        if (fieldTypeEntries != null) {
            for (AvroFieldTypeEntry avroFieldTypeEntry : fieldTypeEntries) {
                recordType.addFieldTypeEntry(convert(avroFieldTypeEntry));
            }
        }
        GenericArray<AvroMixin> mixins = avroRecordType.mixins;
        if (mixins != null) {
            for (AvroMixin avroMixin : mixins) {
                recordType.addMixin(convert(avroMixin.recordTypeId), avroMixin.recordTypeVersion);
            }
        }
        return recordType;
    }

    public AvroRecordType convert(RecordType recordType) {
        AvroRecordType avroRecordType = new AvroRecordType();
        avroRecordType.id = new Utf8(recordType.getId());
        Long version = recordType.getVersion();
        if (version != null) {
            avroRecordType.version = version;
        }
        Collection<FieldTypeEntry> fieldTypeEntries = recordType.getFieldTypeEntries();
        avroRecordType.fieldTypeEntries = new GenericData.Array<AvroFieldTypeEntry>(fieldTypeEntries.size(), Schema.createArray(AvroFieldTypeEntry.SCHEMA$));
        for (FieldTypeEntry fieldTypeEntry : fieldTypeEntries) {
            avroRecordType.fieldTypeEntries.add(convert(fieldTypeEntry));
        }
        Set<Entry<String,Long>> mixinEntries = recordType.getMixins().entrySet();
        avroRecordType.mixins = new GenericData.Array<AvroMixin>(mixinEntries.size(), Schema.createArray(AvroMixin.SCHEMA$));
        for (Entry<String, Long> mixinEntry : mixinEntries) {
            avroRecordType.mixins.add(convert(mixinEntry));
        }
        return avroRecordType;
    }

    public ValueType convert(AvroValueType valueType) {
        return typeManager.getValueType(convert(valueType.primitiveValueType), valueType.multivalue, valueType.hierarchical);
    }

    public AvroValueType convert(ValueType valueType) {
        AvroValueType avroValueType = new AvroValueType();
        avroValueType.primitiveValueType = new Utf8(valueType.getPrimitive().getName());
        avroValueType.multivalue = valueType.isMultiValue();
        avroValueType.hierarchical = valueType.isHierarchical();
        return avroValueType;
    }

    public QName convert(AvroQName name) {
        return new QName(convert(name.namespace), convert(name.name));
    }

    public AvroQName convert(QName name) {
        if (name == null)
            return null;

        AvroQName avroQName = new AvroQName();
        if (name.getNamespace() != null) {
            avroQName.namespace = new Utf8(name.getNamespace());
        }
        avroQName.name = new Utf8(name.getName());
        return avroQName;
    }

    public AvroMixin convert(Entry<String, Long> mixinEntry) {
        AvroMixin avroMixin = new AvroMixin();
        avroMixin.recordTypeId = new Utf8(mixinEntry.getKey());
        Long version = mixinEntry.getValue();
        if (version != null) {
            avroMixin.recordTypeVersion = version;
        }
        return avroMixin;
    }

    public FieldTypeEntry convert(AvroFieldTypeEntry avroFieldTypeEntry) {
        return typeManager.newFieldTypeEntry(convert(avroFieldTypeEntry.id), avroFieldTypeEntry.mandatory);
    }

    public AvroFieldTypeEntry convert(FieldTypeEntry fieldTypeEntry) {
        AvroFieldTypeEntry avroFieldTypeEntry = new AvroFieldTypeEntry();
        avroFieldTypeEntry.id = new Utf8(fieldTypeEntry.getFieldTypeId());
        avroFieldTypeEntry.mandatory = fieldTypeEntry.isMandatory();
        return avroFieldTypeEntry;
    }

    public RuntimeException convert(AvroGenericException avroException) {
        RuntimeException exception = new RuntimeException();
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public AvroGenericException convertOtherException(Throwable throwable) {
        AvroGenericException avroException = new AvroGenericException();
        avroException.remoteCauses = buildCauses(throwable);
        return avroException;
    }

    public RemoteException convert(AvroRemoteException exception) {
        return new RemoteException(exception.getMessage(), exception);
    }

    public AvroRecordException convert(RecordException exception) {
        AvroRecordException avroException = new AvroRecordException();
        avroException.message = new Utf8(exception.getMessage());
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public AvroTypeException convert(TypeException exception) {
        AvroTypeException avroException = new AvroTypeException();
        avroException.message = new Utf8(exception.getMessage());
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public AvroFieldTypeExistsException convert(FieldTypeExistsException exception) {
        AvroFieldTypeExistsException avroFieldTypeExistsException = new AvroFieldTypeExistsException();
        avroFieldTypeExistsException.fieldType = convert(exception.getFieldType());
        avroFieldTypeExistsException.remoteCauses = buildCauses(exception);
        return avroFieldTypeExistsException;
    }

    public FieldTypeExistsException convert(AvroFieldTypeExistsException avroException) {
        FieldTypeExistsException exception = new FieldTypeExistsException(convert(avroException.fieldType));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public AvroRecordTypeExistsException convert(RecordTypeExistsException exception) {
        AvroRecordTypeExistsException avroException = new AvroRecordTypeExistsException();
        avroException.recordType = convert(exception.getRecordType());
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public AvroRecordTypeNotFoundException convert(RecordTypeNotFoundException exception) {
        AvroRecordTypeNotFoundException avroException = new AvroRecordTypeNotFoundException();
        avroException.id = new Utf8(exception.getId());
        Long version = exception.getVersion();
        if (version != null) {
            avroException.version = version;
        }
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public AvroFieldTypeNotFoundException convert(FieldTypeNotFoundException exception) {
        AvroFieldTypeNotFoundException avroException = new AvroFieldTypeNotFoundException();
        if (exception.getId() != null)
            avroException.id = new Utf8(exception.getId());
        if (exception.getName() != null)
            avroException.name = convert(exception.getName());
        Long version = exception.getVersion();
        if (version != null) {
            avroException.version = version;
        }
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public RecordException convert(AvroRecordException avroException) {
        RecordException exception = new RecordException(convert(avroException.message));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public TypeException convert(AvroTypeException avroException) {
        TypeException exception = new TypeException(convert(avroException.message));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public RecordTypeExistsException convert(AvroRecordTypeExistsException avroException) {
        RecordTypeExistsException exception = new RecordTypeExistsException(convert(avroException.recordType));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public RecordTypeNotFoundException convert(AvroRecordTypeNotFoundException avroException) {
        RecordTypeNotFoundException exception = new RecordTypeNotFoundException(convert(avroException.id), avroException.version);
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public FieldTypeNotFoundException convert(AvroFieldTypeNotFoundException avroException) {
        FieldTypeNotFoundException exception;
        if (avroException.id != null) {
            exception = new FieldTypeNotFoundException(convert(avroException.id), avroException.version);
        } else {
            exception = new FieldTypeNotFoundException(convert(avroException.name), avroException.version);
        }
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public FieldTypeUpdateException convert(AvroFieldTypeUpdateException avroException) {
        FieldTypeUpdateException exception = new FieldTypeUpdateException(convert(avroException.message));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public AvroFieldTypeUpdateException convert(FieldTypeUpdateException exception) {
        AvroFieldTypeUpdateException avroException = new AvroFieldTypeUpdateException();
        if (exception.getMessage() != null) {
            avroException.message = new Utf8(exception.getMessage());
        }
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public AvroRecordExistsException convert(RecordExistsException exception)
            throws AvroFieldTypeNotFoundException, AvroTypeException {

        AvroRecordExistsException avroException = new AvroRecordExistsException();
        avroException.record = convert(exception.getRecord());
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
        
    }

    public AvroRecordNotFoundException convert(RecordNotFoundException exception)
            throws AvroFieldTypeNotFoundException, AvroTypeException {

        AvroRecordNotFoundException avroException = new AvroRecordNotFoundException();
        avroException.record = convert(exception.getRecord());
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public AvroVersionNotFoundException convert(VersionNotFoundException exception)
            throws AvroFieldTypeNotFoundException, AvroTypeException {

        AvroVersionNotFoundException avroException = new AvroVersionNotFoundException();
        avroException.record = convert(exception.getRecord());
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public AvroInvalidRecordException convert(InvalidRecordException exception)
            throws AvroFieldTypeNotFoundException, AvroTypeException {

        AvroInvalidRecordException avroException = new AvroInvalidRecordException();
        avroException.record = convert(exception.getRecord());
        if (exception.getMessage() != null) {
            avroException.message = new Utf8(exception.getMessage());
        }
        avroException.remoteCauses = buildCauses(exception);
        return avroException;
    }

    public RecordExistsException convert(AvroRecordExistsException avroException) {
        RecordExistsException exception = new RecordExistsException(convert(avroException.record));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public RecordNotFoundException convert(AvroRecordNotFoundException avroException) {
        RecordNotFoundException exception = new RecordNotFoundException(convert(avroException.record));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public VersionNotFoundException convert(AvroVersionNotFoundException avroException) {
        VersionNotFoundException exception = new VersionNotFoundException(convert(avroException.record));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }

    public InvalidRecordException convert(AvroInvalidRecordException avroException) {
        InvalidRecordException exception = new InvalidRecordException(convert(avroException.record), convert(avroException.message));
        restoreCauses(avroException.remoteCauses, exception);
        return exception;
    }
    
    public String convert(Utf8 utf8) {
        if (utf8 == null) return null;
        return utf8.toString();
    }

    public Utf8 convert(String string) {
        if (string == null) return null;
        return new Utf8(string);
    }

    private GenericArray<AvroExceptionCause> buildCauses(Throwable throwable) {
        GenericData.Array<AvroExceptionCause> causes = new GenericData.Array<AvroExceptionCause>(3, Schema.createArray(AvroExceptionCause.SCHEMA$));

        Throwable cause = throwable;

        while (cause != null) {
            causes.add(convertCause(cause));
            cause = cause.getCause();
        }

        return causes;
    }

    private AvroExceptionCause convertCause(Throwable throwable) {
        AvroExceptionCause cause = new AvroExceptionCause();
        cause.className = convert(throwable.getClass().getName());
        cause.message = convert(throwable.getMessage());

        StackTraceElement[] stackTrace = throwable.getStackTrace();

        cause.stackTrace = new GenericData.Array<AvroStackTraceElement>(stackTrace.length, Schema.createArray(AvroStackTraceElement.SCHEMA$));

        for (StackTraceElement el : stackTrace) {
            cause.stackTrace.add(convert(el));
        }

        return cause;
    }

    private AvroStackTraceElement convert(StackTraceElement el) {
        AvroStackTraceElement result = new AvroStackTraceElement();
        result.className = convert(el.getClassName());
        result.methodName = convert(el.getMethodName());
        result.fileName = convert(el.getFileName());
        result.lineNumber = el.getLineNumber();
        return result;
    }

    private void restoreCauses(GenericArray<AvroExceptionCause> remoteCauses, Throwable throwable) {
        Throwable causes = restoreCauses(remoteCauses);
        if (causes != null) {
            throwable.initCause(causes);
        }
    }

    private Throwable restoreCauses(GenericArray<AvroExceptionCause> remoteCauses) {
        Throwable result = null;

        for (AvroExceptionCause remoteCause : remoteCauses) {
            List<StackTraceElement> stackTrace = new ArrayList<StackTraceElement>((int)remoteCause.stackTrace.size());

            for (AvroStackTraceElement el : remoteCause.stackTrace) {
                stackTrace.add(new StackTraceElement(convert(el.className), convert(el.methodName),
                        convert(el.fileName), el.lineNumber));
            }

            RestoredException cause = new RestoredException(convert(remoteCause.message),
                    convert(remoteCause.className), stackTrace);

            if (result == null) {
                result = cause;
            } else {
                result.initCause(cause);
                result = cause;
            }
        }

        return result;
    }

}

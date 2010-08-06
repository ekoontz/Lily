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
package org.lilycms.repository.impl.test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.RecordTypeNotFoundException;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.api.TypeManager;

public abstract class AbstractTypeManagerRecordTypeTest {

    protected static TypeManager typeManager;
    private static FieldType fieldType1;
    private static FieldType fieldType2;
    private static FieldType fieldType3;

    protected static void setupFieldTypes() throws Exception {
        fieldType1 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING", false, false), new QName("ns1", "field1"), Scope.NON_VERSIONED));
        fieldType2 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("INTEGER", false, false), new QName(null, "field2"), Scope.VERSIONED));
        fieldType3 = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("BOOLEAN", false, false), new QName("ns1", "field3"), Scope.VERSIONED_MUTABLE));
    }

    @Test
    public void testCreateEmpty() throws Exception {
        QName name = new QName("testNS", "testCreateEmpty");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        RecordType recordType2 = typeManager.getRecordTypeByName(name, null);
        assertEquals(recordType, recordType2);
    }

    @Test
    public void testCreate() throws Exception {
        QName name = new QName("testNS", "testCreate");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        RecordType createdRecordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), createdRecordType.getVersion());
        
        recordType.setVersion(Long.valueOf(1));
        recordType.setId(createdRecordType.getId());
        assertEquals(recordType, typeManager.getRecordTypeById(createdRecordType.getId(), null));
    }

    @Test
    public void testUpdate() throws Exception {
        QName name = new QName(null, "testUpdate");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        RecordType recordTypeV1 = typeManager.getRecordTypeByName(name, null);
        assertEquals(Long.valueOf(1), typeManager.updateRecordType(recordTypeV1).getVersion());
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        RecordType recordTypeV2 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordTypeV2.getVersion());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordTypeV2).getVersion());
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), true));
        RecordType recordTypeV3 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(3), recordTypeV3.getVersion());
        assertEquals(Long.valueOf(3), typeManager.updateRecordType(recordType).getVersion());
    
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), true));
        RecordType recordTypeV4 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(4), recordTypeV4.getVersion());
        assertEquals(Long.valueOf(4), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(4));
        assertEquals(recordType, typeManager.getRecordTypeByName(name, null));
        
        // Read old versions
        assertEquals(recordTypeV1, typeManager.getRecordTypeByName(name,Long.valueOf(1)));
        assertEquals(recordTypeV2, typeManager.getRecordTypeByName(name,Long.valueOf(2)));
        assertEquals(recordTypeV3, typeManager.getRecordTypeByName(name,Long.valueOf(3)));
        assertEquals(recordTypeV4, typeManager.getRecordTypeByName(name,Long.valueOf(4)));
    }

    @Test
    public void testReadNonExistingRecordTypeFails() throws Exception {
        QName name = new QName("testNS", "testReadNonExistingRecordTypeFails");
        try {
            typeManager.getRecordTypeByName(name, null);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
        
        typeManager.createRecordType(typeManager.newRecordType(name));
        try {
            typeManager.getRecordTypeByName(name, Long.valueOf(2));
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testUpdateNonExistingRecordTypeFails() throws Exception {
        QName name = new QName("testNS", "testUpdateNonExistingRecordTypeFails");
        RecordType recordType = typeManager.newRecordType(name);
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
        recordType.setId(UUID.randomUUID().toString());
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testFieldTypeExistsOnCreate() throws Exception {
        QName name = new QName("testNS", "testUpdateNonExistingRecordTypeFails");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(UUID.randomUUID().toString(), false));
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
    }

    @Test
    public void testFieldTypeExistsOnUpdate() throws Exception {
        QName name = new QName("testNS", "testFieldGroupExistsOnUpdate");
        RecordType recordType = typeManager.newRecordType(name);
        recordType = typeManager.createRecordType(recordType);
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(UUID.randomUUID().toString(), false));
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
    }

    @Test
    public void testRemove() throws Exception {
        QName name = new QName("testNS", "testRemove");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType = typeManager.createRecordType(recordType);
        
        recordType.removeFieldTypeEntry(fieldType1.getId());
        recordType.removeFieldTypeEntry(fieldType2.getId());
        recordType.removeFieldTypeEntry(fieldType3.getId());
        typeManager.updateRecordType(recordType);
        
        RecordType readRecordType = typeManager.getRecordTypeByName(name, null);
        assertTrue(readRecordType.getFieldTypeEntries().isEmpty());
    }

    @Test
    public void testRemoveLeavesOlderVersionsUntouched() throws Exception {
        QName name = new QName("testNS", "testRemoveLeavesOlderVersionsUntouched");
        RecordType recordType = typeManager.newRecordType(name);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        recordType = typeManager.createRecordType(recordType);
        
        recordType.removeFieldTypeEntry(fieldType1.getId());
        recordType.removeFieldTypeEntry(fieldType2.getId());
        recordType.removeFieldTypeEntry(fieldType3.getId());
        typeManager.updateRecordType(recordType);
        
        RecordType readRecordType = typeManager.getRecordTypeByName(name, Long.valueOf(1));
        assertEquals(3, readRecordType.getFieldTypeEntries().size());
    }

    @Test
    public void testMixin() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixin");
        RecordType mixinType = typeManager.newRecordType(mixinName);
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType = typeManager.createRecordType(mixinType);

        QName recordName = new QName("recordNS", "testMixin");
        RecordType recordType = typeManager.newRecordType(recordName);
        recordType.addMixin(mixinType.getId(), mixinType.getVersion());
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        assertEquals(recordType, typeManager.getRecordTypeById(recordType.getId(), null));
    }
    
    @Test
    public void testMixinLatestVersion() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixinLatestVersion");
        RecordType mixinType = typeManager.newRecordType(mixinName);
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType = typeManager.createRecordType(mixinType);

        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType = typeManager.updateRecordType(mixinType);

        QName recordName = new QName("recordNS", "testMixinLatestVersion");
        RecordType recordType = typeManager.newRecordType(recordName);
        recordType.addMixin(mixinType.getId());
        recordType = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordType.getVersion());
        
        recordType.addMixin(mixinType.getId(), 2L); // Assert latest version of the Mixin RecordType got filled in
        assertEquals(recordType, typeManager.getRecordTypeById(recordType.getId(), null));
    }

    @Test
    public void testMixinUpdate() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixinUpdate");
        RecordType mixinType1 = typeManager.newRecordType(mixinName);
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType1 = typeManager.createRecordType(mixinType1);
        QName mixinName2 = new QName("mixinNS", "testMixinUpdate2");
        RecordType mixinType2 = typeManager.newRecordType(mixinName2);
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        QName recordName = new QName("recordNS", "testMixinUpdate");
        RecordType recordType = typeManager.newRecordType(recordName);
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        recordType = typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        recordType = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordType.getVersion());
        assertEquals(recordType, typeManager.getRecordTypeById(recordType.getId(), null));
    }

    @Test
    public void testMixinRemove() throws Exception {
        QName mixinName = new QName("mixinNS", "testMixinRemove");
        RecordType mixinType1 = typeManager.newRecordType(mixinName);
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType1 = typeManager.createRecordType(mixinType1);
        QName mixinName2 = new QName("mixinNS", "testMixinRemove2");
        RecordType mixinType2 = typeManager.newRecordType(mixinName2);
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        QName recordTypeName = new QName("recordNS", "testMixinRemove");
        RecordType recordType = typeManager.newRecordType(recordTypeName);
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        recordType = typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        recordType.removeMixin(mixinType1.getId());
        recordType = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordType.getVersion());
        RecordType readRecordType = typeManager.getRecordTypeById(recordType.getId(), null);
        Map<String, Long> mixins = readRecordType.getMixins();
        assertEquals(1, mixins.size());
        assertEquals(Long.valueOf(1), mixins.get(mixinType2.getId()));
    }
    
    @Test
    public void testGetRecordTypes() throws Exception {
        RecordType recordType = typeManager.createRecordType(typeManager.newRecordType(new QName("NS", "getRecordTypes")));
        Collection<RecordType> recordTypes = typeManager.getRecordTypes();
        assertTrue(recordTypes.contains(recordType));
    }
    
    @Test
    public void testGetFieldTypes() throws Exception {
        FieldType fieldType = typeManager.createFieldType(typeManager.newFieldType(typeManager.getValueType("STRING", false, false), new QName("NS", "getFieldTypes"), Scope.NON_VERSIONED));
        Collection<FieldType> fieldTypes = typeManager.getFieldTypes();
        assertTrue(fieldTypes.contains(fieldType));
    }

}

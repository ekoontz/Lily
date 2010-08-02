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

import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.lilycms.repository.api.FieldType;
import org.lilycms.repository.api.QName;
import org.lilycms.repository.api.RecordType;
import org.lilycms.repository.api.Scope;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.FieldTypeNotFoundException;
import org.lilycms.repository.api.RecordTypeNotFoundException;

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
        String id = "testCreateEmpty";
        RecordType recordType = typeManager.newRecordType(id);
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(1));
        RecordType recordType2 = typeManager.getRecordType(id, null);
        assertEquals(recordType, recordType2);
    }

    @Test
    public void testCreate() throws Exception {
        String id = "testCreate";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(1));
        assertEquals(recordType, typeManager.getRecordType(id, null));
    }

    @Test
    public void testUpdate() throws Exception {
        String id = "testUpdate";
        RecordType recordType = typeManager.newRecordType(id);
        RecordType recordTypeV1 = typeManager.createRecordType(recordType);
        assertEquals(Long.valueOf(1), recordTypeV1.getVersion());
        assertEquals(Long.valueOf(1), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), true));
        RecordType recordTypeV2 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(2), recordTypeV2.getVersion());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), true));
        RecordType recordTypeV3 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(3), recordTypeV3.getVersion());
        assertEquals(Long.valueOf(3), typeManager.updateRecordType(recordType).getVersion());
    
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), true));
        RecordType recordTypeV4 = typeManager.updateRecordType(recordType);
        assertEquals(Long.valueOf(4), recordTypeV4.getVersion());
        assertEquals(Long.valueOf(4), typeManager.updateRecordType(recordType).getVersion());
        
        recordType.setVersion(Long.valueOf(4));
        assertEquals(recordType, typeManager.getRecordType(id, null));
        
        // Read old versions
        assertEquals(recordTypeV1, typeManager.getRecordType(id,Long.valueOf(1)));
        assertEquals(recordTypeV2, typeManager.getRecordType(id,Long.valueOf(2)));
        assertEquals(recordTypeV3, typeManager.getRecordType(id,Long.valueOf(3)));
        assertEquals(recordTypeV4, typeManager.getRecordType(id,Long.valueOf(4)));
    }

    @Test
    public void testReadNonExistingRecordTypeFails() throws Exception {
        String id = "testReadNonExistingRecordTypeFails";
        try {
            typeManager.getRecordType(id, null);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
        
        typeManager.createRecordType(typeManager.newRecordType(id));
        try {
            typeManager.getRecordType(id, Long.valueOf(2));
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testUpdateNonExistingRecordTypeFails() throws Exception {
        String id = "testUpdateNonExistingRecordTypeFails";
        RecordType recordType = typeManager.newRecordType(id);
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (RecordTypeNotFoundException expected) {
        }
    }

    @Test
    public void testFieldTypeExistsOnCreate() throws Exception {
        String id = "testUpdateNonExistingRecordTypeFails";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(UUID.randomUUID().toString(), false));
        try {
            typeManager.createRecordType(recordType);
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
    }

    @Test
    public void testFieldGroupExistsOnUpdate() throws Exception {
        String id = "testFieldGroupExistsOnUpdate";
        RecordType recordType = typeManager.newRecordType(id);
        typeManager.createRecordType(recordType);
        
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(UUID.randomUUID().toString(), false));
        try {
            typeManager.updateRecordType(recordType);
            fail();
        } catch (FieldTypeNotFoundException expected) {
        }
    }

    @Test
    public void testRemove() throws Exception {
        String id = "testRemove";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        typeManager.createRecordType(recordType);
        
        recordType.removeFieldTypeEntry(fieldType1.getId());
        recordType.removeFieldTypeEntry(fieldType2.getId());
        recordType.removeFieldTypeEntry(fieldType3.getId());
        typeManager.updateRecordType(recordType);
        
        RecordType readRecordType = typeManager.getRecordType(id, null);
        assertTrue(readRecordType.getFieldTypeEntries().isEmpty());
    }

    @Test
    public void testRemoveLeavesOlderVersionsUntouched() throws Exception {
        String id = "testRemoveLeavesOlderVersionsUntouched";
        RecordType recordType = typeManager.newRecordType(id);
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        recordType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        typeManager.createRecordType(recordType);
        
        recordType.removeFieldTypeEntry(fieldType1.getId());
        recordType.removeFieldTypeEntry(fieldType2.getId());
        recordType.removeFieldTypeEntry(fieldType3.getId());
        typeManager.updateRecordType(recordType);
        
        RecordType readRecordType = typeManager.getRecordType(id, Long.valueOf(1));
        assertEquals(3, readRecordType.getFieldTypeEntries().size());
    }

    @Test
    public void testMixin() throws Exception {
        String id = "testMixin";
        RecordType mixinType = typeManager.newRecordType(id+"MIX");
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType = typeManager.createRecordType(mixinType);
        
        RecordType recordType = typeManager.newRecordType(id+"RT");
        recordType.addMixin(mixinType.getId(), mixinType.getVersion());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        recordType.setVersion(Long.valueOf(1));
        assertEquals(recordType, typeManager.getRecordType(recordType.getId(), null));
    }
    
    @Test
    public void testMixinLatestVersion() throws Exception {
        String id = "testMixinLatestVersion";
        RecordType mixinType = typeManager.newRecordType(id+"MIX");
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType = typeManager.createRecordType(mixinType);

        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType = typeManager.updateRecordType(mixinType);
        
        RecordType recordType = typeManager.newRecordType(id+"RT");
        recordType.addMixin(mixinType.getId());
        assertEquals(Long.valueOf(1), typeManager.createRecordType(recordType).getVersion());
        recordType.setVersion(Long.valueOf(1));
        
        recordType.addMixin(mixinType.getId(), 2L); // Assert latest version of the Mixin RecordType got filled in
        assertEquals(recordType, typeManager.getRecordType(recordType.getId(), null));
    }

    @Test
    public void testMixinUpdate() throws Exception {
        String id = "testMixinUpdate";
        RecordType mixinType1 = typeManager.newRecordType(id+"MIX");
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType1 = typeManager.createRecordType(mixinType1);
        RecordType mixinType2 = typeManager.newRecordType(id+"MIX2");
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        RecordType recordType = typeManager.newRecordType(id+"RT");
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordType).getVersion());
        recordType.setVersion(Long.valueOf(2));
        assertEquals(recordType, typeManager.getRecordType(recordType.getId(), null));
    }

    @Test
    public void testMixinRemove() throws Exception {
        String id = "testMixinRemove";
        RecordType mixinType1 = typeManager.newRecordType(id+"MIX");
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType1.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType1 = typeManager.createRecordType(mixinType1);
        RecordType mixinType2 = typeManager.newRecordType(id+"MIX2");
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType1.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType2.getId(), false));
        mixinType2.addFieldTypeEntry(typeManager.newFieldTypeEntry(fieldType3.getId(), false));
        mixinType2 = typeManager.createRecordType(mixinType2);
        
        RecordType recordType = typeManager.newRecordType(id+"RT");
        recordType.addMixin(mixinType1.getId(), mixinType1.getVersion());
        typeManager.createRecordType(recordType);
        
        recordType.addMixin(mixinType2.getId(), mixinType2.getVersion());
        recordType.removeMixin(mixinType1.getId());
        assertEquals(Long.valueOf(2), typeManager.updateRecordType(recordType).getVersion());
        RecordType readRecordType = typeManager.getRecordType(recordType.getId(), null);
        Map<String, Long> mixins = readRecordType.getMixins();
        assertEquals(1, mixins.size());
        assertEquals(Long.valueOf(1), mixins.get(mixinType2.getId()));
    }

}

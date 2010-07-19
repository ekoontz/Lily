package org.lilycms.repository.impl.test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.FieldTypeExistsException;
import org.lilycms.repository.api.FieldTypeUpdateException;

public abstract class AbstractTypeManagerFieldTypeTest {

    protected static TypeManager typeManager;
    
    @Test
    public void testCreate() throws Exception {
        QName name = new QName(null, "testCreate");
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldType fieldType = typeManager.newFieldType(valueType , name, Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        assertEquals(fieldType, typeManager.getFieldTypeById(fieldType.getId()));
        assertEquals(fieldType, typeManager.getFieldTypeByName(fieldType.getName()));
        assertEquals(typeManager.getFieldTypeById(fieldType.getId()), typeManager.getFieldTypeByName(name));
    }

    @Test
    public void testCreateIgnoresGivenId() throws Exception {
        String id = "IgnoreId";
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldType fieldType = typeManager.newFieldType(id, valueType , new QName(null, "aName"), Scope.VERSIONED_MUTABLE);
        fieldType = typeManager.createFieldType(fieldType);
        assertFalse(fieldType.getId().equals(id));
    }

    @Test
    public void testCreateSameNameFails() throws Exception {
        QName name = new QName(null, "testCreateSameNameFails");
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldType fieldType = typeManager.newFieldType(valueType , name, Scope.NON_VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        
        ValueType valueType2 = typeManager.getValueType("INTEGER", false, false);
        FieldType fieldType2 = typeManager.newFieldType(valueType2, name, Scope.NON_VERSIONED);
        try {
            fieldType = typeManager.createFieldType(fieldType2);
            fail();
        } catch (FieldTypeExistsException expected) {
        }
    }

    @Test
    public void testUpdate() throws Exception {
        QName name = new QName(null, "testUpdate");
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldType fieldTypeCreate = typeManager.newFieldType(valueType , name, Scope.VERSIONED);
        fieldTypeCreate = typeManager.createFieldType(fieldTypeCreate);
        
        // Update name
        FieldType fieldTypeNewName = typeManager.newFieldType(fieldTypeCreate.getId(), valueType , new QName(null, "newName"), Scope.VERSIONED);
        fieldTypeNewName = typeManager.updateFieldType(fieldTypeNewName);
        assertEquals(fieldTypeCreate.getId(), fieldTypeNewName.getId());
        assertEquals(fieldTypeNewName, typeManager.getFieldTypeById(fieldTypeCreate.getId()));
        assertEquals(typeManager.getFieldTypeById(fieldTypeCreate.getId()), typeManager.getFieldTypeByName(new QName(null, "newName")));
        
        // Create new fieldType with first name
        ValueType valueType2 = typeManager.getValueType("INTEGER", false, false);
        FieldType fieldType2 = typeManager.newFieldType(valueType2 , name, Scope.NON_VERSIONED);
        fieldType2 = typeManager.createFieldType(fieldTypeCreate);
        assertEquals(fieldType2, typeManager.getFieldTypeByName(name));
    }

    @Test
    public void testUpdateValueTypeFails() throws Exception {
        QName name = new QName(null, "testUpdateValueTypeFails");
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldType fieldType = typeManager.newFieldType(valueType , name, Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        
        fieldType.setValueType(typeManager.getValueType("INTEGER", false, false));
        try {
            typeManager.updateFieldType(fieldType);
            fail("Changing the valueType of a fieldType is not allowed.");
        } catch (FieldTypeUpdateException e) {
        }
    }

    @Test
    public void testUpdateScopeFails() throws Exception {
        QName name = new QName(null, "testUpdateScopeFails");
        ValueType valueType = typeManager.getValueType("STRING", false, false);
        FieldType fieldType = typeManager.newFieldType(valueType , name, Scope.VERSIONED);
        fieldType = typeManager.createFieldType(fieldType);
        
        fieldType.setScope(Scope.NON_VERSIONED);
        try {
            typeManager.updateFieldType(fieldType);
            fail("Changing the scope of a fieldType is not allowed.");
        } catch (FieldTypeUpdateException e) {
        }
    }

}

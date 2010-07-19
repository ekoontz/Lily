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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.impl.IdGeneratorImpl;


public class IdGeneratorImplTest {

    @Test
    //Just to be sure.
    public void testRandomUUID()   {  
        UUID newUUid = UUID.randomUUID();
        String uuidString = newUUid.toString();
        UUID uuidFromString = UUID.fromString(uuidString);
        assertEquals(uuidFromString.toString(), uuidString);
    }  

    @Test
    public void testIdGeneratorDefault() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId recordId = idGenerator.newRecordId();
        assertEquals(recordId, idGenerator.fromBytes(recordId.toBytes()));
        assertEquals(recordId, idGenerator.fromString(recordId.toString()));
    }

    @Test
    public void testIdGeneratorUUID() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        String uuidRecordIDString = "UUID.d27cdb6e-ae6d-11cf-96b8-444553540000";
        
        assertEquals(uuidRecordIDString, idGenerator.fromString(uuidRecordIDString).toString());
        
        byte[] uuidRecordIdBytes = new byte[] {-46, 124, -37, 110, -82, 109, 17, -49, -106, -72, 68, 69, 83, 84, 0, 0, 1};
        assertArrayEquals(uuidRecordIdBytes, idGenerator.fromBytes(uuidRecordIdBytes).toBytes());
        
        assertEquals(uuidRecordIDString, idGenerator.fromBytes(uuidRecordIdBytes).toString());
        
    }
    
    @Test
    public void testIdGeneratorUSER() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId newRecordId = idGenerator.newRecordId("aUserId");
        String userRecordIDString = "USER.aUserId";
        
        assertEquals(newRecordId, idGenerator.fromString(userRecordIDString));
        
        assertEquals(userRecordIDString, idGenerator.fromString(userRecordIDString).toString());
        byte[] userRecordIdBytes = new byte[] {97, 85, 115, 101, 114, 73, 100, 0};
        assertArrayEquals(userRecordIdBytes, idGenerator.fromBytes(userRecordIdBytes).toBytes());
        
        assertEquals(userRecordIDString, idGenerator.fromBytes(userRecordIdBytes).toString());
    }
    
    @Test
    public void testIdGeneratorVARIANT() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId masterRecordId = idGenerator.newRecordId();
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dim1", "dimvalue1");
        RecordId variantRecordId = idGenerator.newRecordId(masterRecordId, variantProperties);

        String variantRecordIdString = masterRecordId.toString() + ".dim1=dimvalue1";
        
        assertEquals(variantRecordId, idGenerator.fromString(variantRecordIdString));
        
        assertEquals(variantRecordIdString, idGenerator.fromString(variantRecordIdString).toString());
        
        assertEquals(variantRecordId, idGenerator.fromBytes(variantRecordId.toBytes()));
    }
    
    @Test
    public void testIdGeneratorVARIANTMultipleProperties() {
        IdGenerator idGenerator = new IdGeneratorImpl();
        RecordId masterRecordId = idGenerator.newRecordId();
        Map<String, String> variantProperties = new HashMap<String, String>();
        variantProperties.put("dim1", "dimvalue1");
        variantProperties.put("dim2", "dimvalue2");

        RecordId variantRecordId = idGenerator.newRecordId(masterRecordId, variantProperties);

        String variantRecordIdString = masterRecordId.toString() + ".dim1=dimvalue1;dim2=dimvalue2";
        assertEquals(variantRecordId, idGenerator.fromString(variantRecordIdString));
        
        assertEquals(variantRecordIdString, idGenerator.fromString(variantRecordIdString).toString());
        
        assertEquals(variantRecordId, idGenerator.fromBytes(variantRecordId.toBytes()));
    }
}

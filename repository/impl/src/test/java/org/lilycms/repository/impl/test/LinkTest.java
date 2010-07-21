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

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.api.Link;
import org.lilycms.repository.api.RecordId;
import org.lilycms.repository.impl.IdGeneratorImpl;

import java.util.HashMap;
import java.util.Map;

public class LinkTest {
    private IdGenerator idGenerator;

    @Before
    public void setUp() throws Exception {
        idGenerator = new IdGeneratorImpl();
    }

    @Test
    public void testPlainRecordId() {
        RecordId recordId = idGenerator.newRecordId("123");

        Link link = Link.newBuilder().recordId(recordId).create();
        assertTrue(link.copyAll());
        assertEquals(recordId, link.getMasterRecordId());

        assertEquals("USER.123", link.toString());
        assertEquals(link, Link.fromString(link.toString(), idGenerator));
        assertEquals(link, Link.fromBytes(link.toBytes(), idGenerator));

        RecordId ctx = idGenerator.newRecordId("0");
        RecordId resolved = link.resolve(ctx, idGenerator);
        assertEquals(recordId, resolved);

        // test the copy all
        Map<String, String> varProps = new HashMap<String, String>();
        varProps.put("a", "1");
        varProps.put("b", "2");

        ctx = idGenerator.newRecordId(ctx, varProps);
        resolved = link.resolve(ctx, idGenerator);
        assertEquals(recordId, resolved.getMaster());
        assertEquals(2, resolved.getVariantProperties().size());
    }

    @Test
    public void testRecordIdWithVarProps() {
        Map<String, String> varProps = new HashMap<String, String>();
        varProps.put("lang", "en");
        varProps.put("branch", "dev");

        RecordId masterRecordId = idGenerator.newRecordId("123");
        RecordId recordId = idGenerator.newRecordId(masterRecordId, varProps);

        Link link = Link.newBuilder().recordId(recordId).copyAll(false).create();
        assertEquals(masterRecordId, link.getMasterRecordId());
        assertEquals("USER.123.!*;branch=dev;lang=en", link.toString());
        assertEquals(link, Link.fromString(link.toString(), idGenerator));
        assertEquals(link, Link.fromBytes(link.toBytes(), idGenerator));

        assertEquals(2, link.getVariantProps().size());
        assertEquals(Link.PropertyMode.SET, link.getVariantProps().get("lang").getMode());
        assertEquals("en", link.getVariantProps().get("lang").getValue());

        Map<String, String> ctxVarProps = new HashMap<String, String>();
        varProps.put("a", "1");
        varProps.put("b", "2");
        varProps.put("lang", "nl");

        RecordId ctx = idGenerator.newRecordId(idGenerator.newRecordId("0"), ctxVarProps);
        RecordId resolved = link.resolve(ctx, idGenerator);

        // Nothing from the context should have been copied
        assertEquals(2, resolved.getVariantProperties().size());
        assertEquals("en", resolved.getVariantProperties().get("lang"));
        assertEquals("dev", resolved.getVariantProperties().get("branch"));
    }

    @Test
    public void testIndividualRemove() {
        RecordId recordId = idGenerator.newRecordId("123");

        Link link = Link.newBuilder().recordId(recordId).remove("lang").set("x", "1").create();
        assertEquals("USER.123.-lang;x=1", link.toString());
        assertEquals(link, Link.fromString(link.toString(), idGenerator));
        assertEquals(link, Link.fromBytes(link.toBytes(), idGenerator));

        Map<String, String> ctxVarProps = new HashMap<String, String>();
        ctxVarProps.put("lang", "en");
        ctxVarProps.put("branch", "dev");

        RecordId ctx = idGenerator.newRecordId(idGenerator.newRecordId("0"), ctxVarProps);
        RecordId resolved = link.resolve(ctx, idGenerator);

        assertNull(resolved.getVariantProperties().get("lang"));
        assertEquals("dev", resolved.getVariantProperties().get("branch"));
        assertEquals("1", resolved.getVariantProperties().get("x"));
        assertEquals(2, resolved.getVariantProperties().size());
    }

    @Test
    public void testIndividualCopy() {
        RecordId recordId = idGenerator.newRecordId("123");

        Link link = Link.newBuilder().recordId(recordId).copyAll(false).copy("branch").set("x", "1").create();
        assertEquals("USER.123.!*;+branch;x=1", link.toString());
        assertEquals(link, Link.fromString(link.toString(), idGenerator));
        assertEquals(link, Link.fromBytes(link.toBytes(), idGenerator));

        Map<String, String> ctxVarProps = new HashMap<String, String>();
        ctxVarProps.put("lang", "en");
        ctxVarProps.put("branch", "dev");

        RecordId ctx = idGenerator.newRecordId(idGenerator.newRecordId("0"), ctxVarProps);
        RecordId resolved = link.resolve(ctx, idGenerator);

        assertNull(resolved.getVariantProperties().get("lang"));
        assertEquals("dev", resolved.getVariantProperties().get("branch"));
        assertEquals("1", resolved.getVariantProperties().get("x"));
        assertEquals(2, resolved.getVariantProperties().size());
    }

    @Test
    public void testLinkToSelf() {
        Link link = Link.newBuilder().create();
        assertNull(link.getMasterRecordId());
        assertEquals(".", link.toString());
        assertEquals(link, Link.fromString(link.toString(), idGenerator));
        assertEquals(link, Link.fromBytes(link.toBytes(), idGenerator));

        Map<String, String> varProps = new HashMap<String, String>();
        varProps.put("lang", "en");
        varProps.put("branch", "dev");

        RecordId recordId = idGenerator.newRecordId(idGenerator.newRecordId("123"), varProps);

        RecordId resolved = link.resolve(recordId, idGenerator);

        assertEquals(recordId, resolved);
    }

    @Test
    public void testLinkToMaster() {
        Link link = Link.newBuilder().copyAll(false).create();
        assertNull(link.getMasterRecordId());
        assertEquals(".!*", link.toString());
        assertEquals(link, Link.fromString(link.toString(), idGenerator));
        assertEquals(link, Link.fromBytes(link.toBytes(), idGenerator));

        Map<String, String> varProps = new HashMap<String, String>();
        varProps.put("lang", "en");
        varProps.put("branch", "dev");

        RecordId recordId = idGenerator.newRecordId(idGenerator.newRecordId("123"), varProps);

        RecordId resolved = link.resolve(recordId, idGenerator);

        assertEquals(recordId.getMaster(), resolved);
    }

    @Test
    public void testImmutability() {
        Map<String, String> varProps = new HashMap<String, String>();
        varProps.put("lang", "en");
        varProps.put("branch", "dev");

        RecordId masterRecordId = idGenerator.newRecordId("123");
        RecordId recordId = idGenerator.newRecordId(masterRecordId, varProps);

        Link link = Link.newBuilder().recordId(recordId).copyAll(false).create();

        try {
            link.getVariantProps().put("z", null);
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // ok
        }

        try {
            link.getMasterRecordId().getVariantProperties().put("z", "z");
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // ok
        }

        try {
            recordId.getVariantProperties().put("z", "z");
            fail("expected exception");
        } catch (UnsupportedOperationException e) {
            // ok
        }
    }
}

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
package org.lilyproject.indexer.engine.test;

import org.junit.Test;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.InputStream;
import java.net.URL;

/**
 * Tests the indexer conf XML Schema.
 */
public class SchemaTest {
    @Test
    public void test() throws Exception {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL url = getClass().getClassLoader().getResource("org/lilyproject/indexer/conf/indexerconf.xsd");
        Schema schema = factory.newSchema(url);
        Validator validator = schema.newValidator();

        InputStream is = getClass().getClassLoader().getResourceAsStream("org/lilyproject/indexer/test/indexerconf1.xml");
        validator.validate(new StreamSource(is));
    }
}

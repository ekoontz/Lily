package org.lilycms.indexer.test;

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
        URL url = getClass().getClassLoader().getResource("org/lilycms/indexer/conf/indexerconf.xsd");
        Schema schema = factory.newSchema(url);
        Validator validator = schema.newValidator();

        InputStream is = getClass().getClassLoader().getResourceAsStream("org/lilycms/indexer/test/indexerconf1.xml");
        validator.validate(new StreamSource(is));
    }
}

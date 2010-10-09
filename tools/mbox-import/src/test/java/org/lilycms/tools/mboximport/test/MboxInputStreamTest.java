package org.lilycms.tools.mboximport.test;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.lilycms.tools.mboximport.MboxInputStream;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

public class MboxInputStreamTest {
    @Test
    public void test() throws Exception {
        MboxInputStream stream = new MboxInputStream(new ByteArrayInputStream("From someone\nfoobar\nFrom someone else\nanother foobar".getBytes()), 10000);

        stream.nextMessage();
        String msg1 = IOUtils.toString(stream);
        assertEquals("foobar\n", msg1);

        stream.nextMessage();
        String msg2 = IOUtils.toString(stream);
        assertEquals("another foobar\n", msg2);
    }
}

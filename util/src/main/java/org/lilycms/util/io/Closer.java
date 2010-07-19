package org.lilycms.util.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;

public class Closer {
    /**
     * Closes anything {@link Closeable}, catches any throwable that might occur during closing and logs it as an error.
     */
    public static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                Log log = LogFactory.getLog(Closer.class);
                log.error("Error closing something of type " + closeable.getClass().getName(), t);
            }
        }
    }
}

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
package org.lilycms.util.io;

import org.apache.commons.logging.LogFactory;

import java.io.Closeable;

public class IOUtils {
    public static void closeQuietly(Closeable cl) {
        if (cl != null) {
            try {
                cl.close();
            } catch (Throwable t) {
                LogFactory.getLog(IOUtils.class).error("Problem closing a source or destination.", t);
            }
        }
    }

    public static void closeQuietly(Closeable cl, String identification) {
        if (cl != null) {
            try {
                cl.close();
            } catch (Throwable t) {
                LogFactory.getLog(IOUtils.class).error("Problem closing a source or destination on " + identification, t);
            }
        }
    }
}

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
package org.lilyproject.util.hbase.metrics;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * Based upon Hadoop's org.apache.hadoop.metrics.util.MBeanUtil
 */
public class MBeanUtil {
    public static ObjectName registerMBean(final String serviceName,
            final String nameName,
            final Object theMbean) {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = getMBeanName(serviceName, nameName);
        try {
            mbs.registerMBean(theMbean, name);
            return name;
        } catch (InstanceAlreadyExistsException ie) {
            // Ignore if instance already exists
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    static public void unregisterMBean(ObjectName mbeanName) {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (mbeanName == null)
            return;
        try {
            mbs.unregisterMBean(mbeanName);
        } catch (InstanceNotFoundException e ) {
            // ignore
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static private ObjectName getMBeanName(final String serviceName,
            final String nameName) {
        ObjectName name = null;
        try {
            name = new ObjectName("Lily:" +
                    "service=" + serviceName + ",name=" + nameName);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
        return name;
    }
}

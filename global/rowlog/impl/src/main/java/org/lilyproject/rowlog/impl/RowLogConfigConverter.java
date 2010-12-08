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
package org.lilyproject.rowlog.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.rowlog.api.RowLogConfig;
import org.lilyproject.util.json.JsonFormat;
import org.lilyproject.util.json.JsonUtil;

public class RowLogConfigConverter {
    public static RowLogConfigConverter INSTANCE = new RowLogConfigConverter();

    public RowLogConfig fromJsonBytes(String rowLogId, byte[] json) {
        ObjectNode node;
        try {
            node = (ObjectNode)JsonFormat.deserialize(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing row log config JSON. Row log ID " + rowLogId, e);
        }
        return fromJson(rowLogId, node);
    }

    public RowLogConfig fromJson(String rowLogId, ObjectNode node) {
        long lockTimeout = JsonUtil.getLong(node, "lockTimeout");
        boolean respectOrder = JsonUtil.getBoolean(node, "respectOrder");
        boolean enableNotify = JsonUtil.getBoolean(node, "enableNotify");
        long notifyDelay = JsonUtil.getLong(node, "notifyDelay");
        long minimalProcessDelay = JsonUtil.getLong(node, "minimalProcessDelay");

        return new RowLogConfig(lockTimeout, respectOrder, enableNotify, notifyDelay, minimalProcessDelay);
    }

    public byte[] toJsonBytes(String rowLogId, RowLogConfig config) {
        try {
            return JsonFormat.serializeAsBytes(toJson(config));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing row log config to JSON. Row log ID " +
                    rowLogId, e);
        }
    }

    public ObjectNode toJson(RowLogConfig config) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("lockTimeout", config.getLockTimeout());
        node.put("respectOrder", config.isRespectOrder());
        node.put("enableNotify", config.isEnableNotify());
        node.put("notifyDelay", config.getNotifyDelay());
        node.put("minimalProcessDelay", config.getMinimalProcessDelay());

        return node;
    }
}

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
package org.lilycms.server.modules.general;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.kauriproject.conf.Conf;

import java.io.IOException;

public class Metrics {
    public Metrics(Conf conf) throws IOException {
        ContextFactory contextFactory = ContextFactory.getFactory();

        for (Conf attr : conf.getChild("hadoopMetricsAttributes").getChildren("attribute")) {
            contextFactory.setAttribute(attr.getAttribute("name"), attr.getAttribute(("value")));
        }

        boolean enableJvmMetrics = conf.getChild("enableJvmMetrics").getValueAsBoolean();
        if (enableJvmMetrics) {
            JvmMetrics.init("lily", "aLilySession");
        }
    }
}

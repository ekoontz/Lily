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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.kauriproject.conf.Conf;

public class HadoopConfigurationFactoryImpl implements HadoopConfigurationFactory {
    private Conf hbaseConf;
    private Conf mrConf;
    private String zkConnectString;
    private int zkSessionTimeout;

    public HadoopConfigurationFactoryImpl(Conf hbaseConf, Conf mrConf, String zkConnectString, int zkSessionTimeout) {
        this.hbaseConf = hbaseConf;
        this.mrConf = mrConf;
        this.zkConnectString = zkConnectString;
        this.zkSessionTimeout = zkSessionTimeout;
    }

    public Configuration getHBaseConf() {
        Configuration hadoopConf = HBaseConfiguration.create();

        for (Conf conf : hbaseConf.getChild("properties").getChildren("property")) {
            String name = conf.getRequiredChild("name").getValue();
            String value = conf.getRequiredChild("value").getValue();
            hadoopConf.set(name, value);
        }

        return hadoopConf;
    }

    public Configuration getMapReduceConf() {
        Configuration hadoopConf = new Configuration();

        for (Conf conf : mrConf.getChild("properties").getChildren("property")) {
            String name = conf.getRequiredChild("name").getValue();
            String value = conf.getRequiredChild("value").getValue();
            hadoopConf.set(name, value);
        }

        return hadoopConf;
    }

    public Configuration getMapReduceJobConf() {
        Configuration hadoopConf = new Configuration();

        for (Conf conf : mrConf.getChild("properties").getChildren("property")) {
            String name = conf.getRequiredChild("name").getValue();
            String value = conf.getRequiredChild("value").getValue();
            hadoopConf.set(name, value);
        }

        for (Conf conf : mrConf.getChild("jobProperties").getChildren("property")) {
            String name = conf.getRequiredChild("name").getValue();
            String value = conf.getRequiredChild("value").getValue();
            hadoopConf.set(name, value);
        }

        return hadoopConf;
    }

    public String getZooKeeperConnectString() {
        return zkConnectString;
    }

    public int getZooKeeperSessionTimeout() {
        return zkSessionTimeout;
    }
}

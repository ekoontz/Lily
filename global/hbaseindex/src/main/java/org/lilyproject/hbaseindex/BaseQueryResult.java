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
package org.lilyproject.hbaseindex;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

abstract class BaseQueryResult implements QueryResult {
    protected Result currentResult;
    protected QueryResult currentQResult;

    public byte[] getData(byte[] qualifier) {
        if (currentResult != null) {
            return currentResult.getValue(Index.DATA_FAMILY, qualifier);
        } else if (currentQResult != null) {
            return currentQResult.getData(qualifier);
        } else {
            throw new RuntimeException("QueryResult.getData() is being called but there is no current result.");
        }
    }

    public byte[] getData(String qualifier) {
        return getData(Bytes.toBytes(qualifier));
    }

    public String getDataAsString(String qualifier) {
        return Bytes.toString(getData(Bytes.toBytes(qualifier)));
    }
}

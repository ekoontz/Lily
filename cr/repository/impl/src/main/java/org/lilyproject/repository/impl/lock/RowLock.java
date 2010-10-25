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
package org.lilyproject.repository.impl.lock;

import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;

public class RowLock {
    private static Random random = new Random();
    
    private final byte[] rowKey;
    private byte[] permit;

    public static RowLock createRowLock(byte[] rowKey) {
        return new RowLock(rowKey, Bytes.add(Bytes.toBytes(System.currentTimeMillis()), Bytes.toBytes(random.nextInt())));
    }
    
    public RowLock(byte[] rowKey, byte[] permit) {
        this.rowKey = rowKey;
        this.permit = permit;
    }
    
    public byte[] getRowKey() {
        return rowKey;
    }
    
    public long getTimestamp() {
        return Bytes.toLong(permit);
    }
    
    public byte[] getPermit() {
        return permit;
    }
}

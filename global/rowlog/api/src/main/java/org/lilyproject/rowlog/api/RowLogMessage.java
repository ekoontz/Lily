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
package org.lilyproject.rowlog.api;

/**
 * The RowLogMessage is the message object that should be put on 
 * the {@link RowLog} and processed by a {@link RowLogMessageListener}
 * 
 * <p> A RowLogMessage is created by the {@link RowLog} when calling {@link RowLog#putMessage(byte[], byte[], byte[], org.apache.hadoop.hbase.client.Put)}
 * 
 * <p> The message should contain the information needed by the consumers to be able to do their work.
 * In other words, the producer of the message and the consumers should agree on the content of the message.
 * 
 * <p> A message is always related to a specific HBase-row and is used to describe an event that happened on the data of that row. 
 * 
 * <p> A RowLogMessage is uniquely identified by the combination of its rowKey, seqNr and timestamp.
 */
public interface RowLogMessage {
    /**
     * Identifies the row to which the message is related. 
     * @return the HBase row key
     */
    byte[] getRowKey();
    
    /**
     * A sequence number used to identify the position of the message in order of the messages that were created (events) for the related row.
     * @return a sequence number , unique within the context of a row
     */
    long getSeqNr();
    
    /**
     * The timestamp of when the message was created. 
     * @return the timestamp
     */
    long getTimestamp();
    
    /**
     * The data field can be used to put extra informative information on the message. 
     * This data will be stored in the message table and should be kept small.
     * @return the data
     */
    byte[] getData();
    
    /**
     * The payload contains all information about a message for a {@link RowLogMessageListener} to be able to process a message.
     * @return the payload
     * @throws RowLogException 
     */
    byte[] getPayload() throws RowLogException;
}

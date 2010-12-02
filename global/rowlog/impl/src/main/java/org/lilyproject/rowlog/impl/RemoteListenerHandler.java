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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.util.io.Closer;

public class RemoteListenerHandler {
    private final Log log = LogFactory.getLog(getClass());
    private final RowLogMessageListener consumer;
    private ServerBootstrap bootstrap;
    private final RowLog rowLog;
    private Channel channel;
    private String listenerId;
    private final String subscriptionId;
    private final RowLogConfigurationManager rowLogConfMgr;

    public RemoteListenerHandler(RowLog rowLog, String subscriptionId, RowLogMessageListener consumer,
            RowLogConfigurationManager rowLogConfMgr) throws RowLogException {
        this.rowLog = rowLog;
        this.subscriptionId = subscriptionId;
        this.consumer = consumer;
        this.rowLogConfMgr = rowLogConfMgr;
        bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("messageDecoder", new MessageDecoder());
                pipeline.addLast("messageHandler", new MessageHandler());
                pipeline.addLast("resultEncoder", new ResultEncoder());
                return pipeline;
            }
        });
    }
    
    public void start() throws RowLogException, InterruptedException, KeeperException {
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RowLogException("Failed to start remote listener", e);
        }
        String hostName = inetAddress.getHostName();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostName, 0);
        channel = bootstrap.bind(inetSocketAddress);
        int port = ((InetSocketAddress)channel.getLocalAddress()).getPort();
        listenerId = hostName + ":" + port;
        rowLogConfMgr.addListener(rowLog.getId(), subscriptionId, listenerId);
    }
    
    public void stop() throws InterruptedException {
        if (channel != null) {
            ChannelFuture future = channel.close();
            try {
                future.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Continue to try to release resources
            }
        }

        bootstrap.releaseExternalResources();

        if (listenerId != null) {
            try {
                rowLogConfMgr.removeListener(rowLog.getId(), subscriptionId, listenerId);
            } catch (KeeperException e) {
                log.warn("Exception while removing listener. Row log ID " + rowLog.getId() + ", subscription ID " + subscriptionId +
                        ", listener ID " + listenerId, e);
            } catch (RowLogException e) {
                log.warn("Exception while removing listener. Row log ID " + rowLog.getId() + ", subscription ID " + subscriptionId +
                        ", listener ID " + listenerId, e);
            }
        }
    }
    
    private class MessageDecoder extends OneToOneDecoder {
        @Override
        protected RowLogMessage decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            ChannelBufferInputStream inputStream = new ChannelBufferInputStream((ChannelBuffer)msg);
            
            long timestamp = inputStream.readLong();
            
            int rowKeyLength = inputStream.readInt();
            byte[] rowKey = new byte[rowKeyLength];
            inputStream.readFully(rowKey, 0, rowKeyLength);
            
            long seqnr = inputStream.readLong();
            
            int dataLength = inputStream.readInt();
            byte[] data = null;
            if (dataLength > 0) {
                data = new byte[dataLength];
                inputStream.readFully(data, 0, dataLength);
            }
            inputStream.close();
            return new RowLogMessageImpl(timestamp, rowKey, seqnr, data, rowLog);
        }
    }
    
    private class MessageHandler extends SimpleChannelUpstreamHandler {
        private Semaphore semaphore = new Semaphore(0);
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            RowLogMessage message = (RowLogMessage)e.getMessage();
            boolean result = consumer.processMessage(message);
            writeResult(e.getChannel(), result);
            semaphore.acquire();
        }

        private void writeResult(Channel channel, boolean result) {
            if (channel.isOpen()) {
                ChannelFuture future = channel.write(new Boolean(result));
                future.addListener(new ChannelFutureListener() {
                    
                    public void operationComplete(ChannelFuture future) throws Exception {
                        semaphore.release();
                    }
                });
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            log.warn("Exception in MessageHandler while processing message, "+ e.getCause());
            writeResult(e.getChannel(), false);
        }
    }
    
    private class ResultEncoder extends OneToOneEncoder {
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            ChannelBuffer channelBuffer = ChannelBuffers.buffer(1);
            ChannelBufferOutputStream outputStream = new ChannelBufferOutputStream(channelBuffer);
            try {
                outputStream.writeBoolean((Boolean)msg);
                return channelBuffer;
            } finally {
                Closer.close(outputStream);
            }
        }
    }
}

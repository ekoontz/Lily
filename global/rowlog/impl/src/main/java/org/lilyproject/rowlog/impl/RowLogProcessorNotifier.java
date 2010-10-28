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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;

public class RowLogProcessorNotifier {
    
    private ClientBootstrap bootstrap;
    private NioClientSocketChannelFactory channelFactory;
    private String[] processorHostAndPort;
    private RowLogConfigurationManager rowLogConfigurationManager;
    private Map<String, Long> wakeupDelays = Collections.synchronizedMap(new HashMap<String, Long>());
    private long delay = 100;

    public RowLogProcessorNotifier(RowLogConfigurationManager rowLogConfigurationManager) {
        this.rowLogConfigurationManager = rowLogConfigurationManager;
    }
    
    protected void notifyProcessor(String rowLogId, String shardId) throws InterruptedException {
        long now = System.currentTimeMillis();
        Long delayUntil = wakeupDelays.get(rowLogId+shardId);
        if (delayUntil == null || now >= delayUntil) {
            Channel channel = getProcessorChannel(rowLogId, shardId);
            if ((channel != null) && (channel.isConnected())) { 
                ChannelBuffer channelBuffer = ChannelBuffers.buffer(1);
                channelBuffer.writeByte(1);
                ChannelFuture writeFuture = channel.write(channelBuffer);
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
            // Wait at least 100ms before sending another notification 
            wakeupDelays.put(rowLogId+shardId, now + delay);
        }
    }
    
    public void close() {
        processorHostAndPort = null;
        if (channelFactory != null) {
            channelFactory.releaseExternalResources();
            channelFactory = null;
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
    
    private Channel getProcessorChannel(String rowLogId, String shardId) throws InterruptedException {
        if (processorHostAndPort == null) {
            String processorHost = rowLogConfigurationManager.getProcessorHost(rowLogId, shardId);
            if (processorHost != null)
                processorHostAndPort = processorHost.split(":");
        }
        if (processorHostAndPort != null) {
            initBootstrap();
            ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(processorHostAndPort[0], Integer.valueOf(processorHostAndPort[1])));
            if (connectFuture.await(1000)) {
                if (connectFuture.isSuccess()) {
                    return connectFuture.getChannel();
                } else {
                    processorHostAndPort = null; // Re-read from Zookeeper next time
                    return null;
                }
                
            }
            processorHostAndPort = null; // Re-read from Zookeeper next time
        }
        return null;
    }
    
    private void initBootstrap() {
        if (bootstrap == null) {
            if (channelFactory == null) {
                channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
            }
            bootstrap = new ClientBootstrap(channelFactory);
            bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                public ChannelPipeline getPipeline() {
                    return Channels.pipeline(new SimpleChannelHandler() {
                        
                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
                            // The connection will not be successful. A new host and port will be read from Zookeeper next time.
                        }
                    });
                }
            });
            bootstrap.setOption("tcpNoDelay", true);
            bootstrap.setOption("keepAlive", true);
        }
    }
}

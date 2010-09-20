package org.lilycms.rowlog.impl;

import java.net.InetSocketAddress;
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
import org.lilycms.rowlog.api.RowLogException;

public class RowLogProcessorNotifier {
    
    private ClientBootstrap bootstrap;
    private NioClientSocketChannelFactory channelFactory;
    private String[] processorHostAndPort;
    private RowLogConfigurationManager rowLogConfigurationManager;

    public RowLogProcessorNotifier(String zkConnectString) throws RowLogException {
        rowLogConfigurationManager = new RowLogConfigurationManager(zkConnectString);
    }
    
    protected void notifyProcessor(String rowLogId, String shardId) {
        Channel channel = getProcessorChannel(rowLogId, shardId);
        if ((channel != null) && (channel.isConnected())) { 
            ChannelBuffer channelBuffer = ChannelBuffers.buffer(1);
            channelBuffer.writeByte(1);
            ChannelFuture writeFuture = channel.write(channelBuffer);
            writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }
    
    protected void close() {
        processorHostAndPort = null;
        if (channelFactory != null) {
            channelFactory.releaseExternalResources();
            channelFactory = null;
        }
        try {
            rowLogConfigurationManager.stop();
        } catch (InterruptedException e) {
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
    
    private Channel getProcessorChannel(String rowLogId, String shardId) {
        if (processorHostAndPort == null) {
            String processorHost = rowLogConfigurationManager.getProcessorHost(rowLogId, shardId);
            if (processorHost != null)
                processorHostAndPort = processorHost.split(":");
        }
        if (processorHostAndPort != null) {
            initBootstrap();
            ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(processorHostAndPort[0], Integer.valueOf(processorHostAndPort[1])));
            try {
                if (connectFuture.await(1000)) {
                    if (connectFuture.isSuccess()) {
                        return connectFuture.getChannel();
                    } else {
                        processorHostAndPort = null; // Re-read from Zookeeper next time
                        return null;
                    }
                    
                }
            } catch (InterruptedException e) {
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

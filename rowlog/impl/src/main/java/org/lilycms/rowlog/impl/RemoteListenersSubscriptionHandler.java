package org.lilycms.rowlog.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public class RemoteListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    private ClientBootstrap bootstrap;
    private NioClientSocketChannelFactory channelFactory;
    private boolean messageProcessSuccess = false;

    
    public RemoteListenersSubscriptionHandler(int subscriptionId, int workerCount, MessagesWorkQueue messagesWorkQueue, RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
        executorService = Executors.newFixedThreadPool(workerCount);
        initBootstrap();
    }
    
    protected boolean processMessage(String host, RowLogMessage message) {
        Channel channel = getListenerChannel(host);
        
        if ((channel != null) && (channel.isConnected())) { 
            channel.write(message);
            ChannelFuture closeFuture = channel.getCloseFuture();
            try {
                closeFuture.await();
            } catch (InterruptedException e) {
                channel.close();
                return false;
            }
        }
        return messageProcessSuccess;
    }
    
    private void initBootstrap() {
        if (bootstrap == null) {
            if (channelFactory == null) {
                channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
            }
            bootstrap = new ClientBootstrap(channelFactory);
            bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                public ChannelPipeline getPipeline() {
                    ChannelPipeline pipeline = Channels.pipeline();
                    pipeline.addLast("resultDecoder", new ResultDecoder());
                    pipeline.addLast("resultHandler", new ResultHandler());
                    pipeline.addLast("messageEncoder", new MessageEncoder());
                    return pipeline;
                }
            });
            bootstrap.setOption("tcpNoDelay", true);
            bootstrap.setOption("keepAlive", true);
        }
    }
    
    private Channel getListenerChannel(String host) {
        String listenerHostAndPort[] = host.split(":");
        ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(listenerHostAndPort[0], Integer.valueOf(listenerHostAndPort[1])));
        try {
            connectFuture.await();
            if (connectFuture.isSuccess()) {
                return connectFuture.getChannel();
            } else {
                return null;
            }
        } catch (InterruptedException e) {
        }
        return null;
    }
    
    private class ResultDecoder extends OneToOneDecoder {
        @Override
        protected Boolean decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            ChannelBufferInputStream inputStream = new ChannelBufferInputStream((ChannelBuffer)msg);
            boolean result = inputStream.readBoolean();
            inputStream.close();
            return result;
        }
    }
    
    private class ResultHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            messageProcessSuccess = (Boolean)e.getMessage();
            e.getChannel().close();
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            // TODO
            e.getChannel().close();
        }
    }
    
    private class MessageEncoder extends OneToOneEncoder {
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            try {
                RowLogMessage message = (RowLogMessage)msg;
                byte[] id = message.getId();
                byte[] rowKey = message.getRowKey();
                byte[] data = message.getData();
                int capacity = 4 + id.length + 4 + rowKey.length + 8 + 4;
                if (data != null)
                    capacity = capacity + data.length;
                ChannelBuffer channelBuffer = ChannelBuffers.buffer(capacity);
                ChannelBufferOutputStream outputStream = new ChannelBufferOutputStream(channelBuffer);
                outputStream.writeInt(id.length);
                outputStream.write(id);
                outputStream.writeInt(rowKey.length);
                outputStream.write(rowKey);
                outputStream.writeLong(message.getSeqNr());
                if (data != null) {
                    outputStream.writeInt(data.length);
                    outputStream.write(data);
                } else {
                    outputStream.writeInt(0);
                }
                outputStream.close();
                return channelBuffer;
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                throw e;
            }
        }
    }
}

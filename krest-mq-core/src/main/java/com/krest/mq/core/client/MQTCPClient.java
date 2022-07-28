package com.krest.mq.core.client;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.MQUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTCPClient{

    private String host;
    private int port;

    ChannelListener inactiveListener;
    private Bootstrap bootstrap;
    public Channel channel;
    EventLoopGroup workGroup = new NioEventLoopGroup();


    private MQTCPClient() {
    }

    public ChannelListener getInactiveListener() {
        return inactiveListener;
    }

    /**
     * 构造方法
     */
    public MQTCPClient(String host, int port, MQMessage.MQEntity mqEntity) {
        this.host = host;
        this.port = port;
        inactiveListener = (mqMessage) -> {
            log.info("connection with server is closed.");
            log.info("try to reconnect to the server.");
            channel = null;
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port);
            }
            while (channel == null);
            channel.writeAndFlush(mqEntity).sync();
        };
    }


    public void sendMsg(MQMessage.MQEntity request) {
        if (channel == null) {
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port);
            }
            while (channel == null);
        }
        try {
            channel.writeAndFlush(request).sync();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }

    public void connect(ChannelInboundHandlerAdapter handlerAdapter) {
        bootstrap = new Bootstrap();
        try {
            bootstrap.group(workGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            // 客户端 -> 解码器
                            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());//解决粘包半包编码器
                            // 加入一个Decoder
                            ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                            ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());//解决粘包半包编码器
                            ch.pipeline().addLast(new ProtobufEncoder());
                            ch.pipeline().addLast(handlerAdapter);
                        }
                    });
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port);
            }
            while (channel == null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void connect(ChannelInitializer channelInitializer) {
        bootstrap = new Bootstrap();
        try {
            bootstrap.group(workGroup)
                    .channel(NioSocketChannel.class)
                    .handler(channelInitializer);
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port);
            }
            while (channel == null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public void connectAndSend(ChannelInboundHandlerAdapter handlerAdapter, MQMessage.MQEntity mqEntity) {
        connect(handlerAdapter);
        sendMsg(mqEntity);

    }


    public void stop() {
        if (null != this.workGroup) {
            this.workGroup.shutdownGracefully();
        }
    }
}

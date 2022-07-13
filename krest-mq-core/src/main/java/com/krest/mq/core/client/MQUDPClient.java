package com.krest.mq.core.client;

import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.MQUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQUDPClient implements MQClient {

    MQConfig mqConfig;
    ChannelListener inactiveListener;
    private Bootstrap bootstrap;
    private Channel channel;
    EventLoopGroup workGroup = new NioEventLoopGroup();


    private MQUDPClient() {
    }

    public ChannelListener getInactiveListener() {
        return inactiveListener;
    }

    /**
     * 构造方法
     */
    public MQUDPClient(MQConfig mqConfig) {
        this.mqConfig = mqConfig;
        inactiveListener = () -> {
            log.info("connection with server is closed.");
            log.info("try to reconnect to the server.");
            channel = null;
            do {
                channel = MQUtils.tryConnect(bootstrap, this.mqConfig.getRemoteAddress(), this.mqConfig.getPort());
            }
            while (channel == null);
        };
    }

    public void sendMsg(MQMessage.MQEntity request) {
        try {
            channel.writeAndFlush(request).sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connect(ChannelInboundHandlerAdapter handlerAdapter) {
        bootstrap = new Bootstrap();
        try {
            bootstrap.group(workGroup)
                    .channel(NioDatagramChannel.class)
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel ch) throws Exception {
                            ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                            ch.pipeline().addLast(new ProtobufEncoder());
                            ch.pipeline().addLast(handlerAdapter.getClass().newInstance());
                        }
                    }).
                    option(ChannelOption.SO_BROADCAST, true);

            bootstrap.bind(this.mqConfig.getPort());
            do {
                channel = MQUtils.tryConnect(bootstrap,
                        this.mqConfig.getRemoteAddress(),
                        this.mqConfig.getRemotePort());
            }
            while (channel == null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
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

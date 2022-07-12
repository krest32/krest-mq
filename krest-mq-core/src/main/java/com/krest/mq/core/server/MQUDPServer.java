package com.krest.mq.core.server;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MQTCPServerHandler;
import com.krest.mq.core.handler.MQUDPServerHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQUDPServer implements MQServer {
    NioEventLoopGroup workGroup;
    boolean isPushMode = false;
    int port;

    private MQUDPServer() {
    }

    public MQUDPServer(int port) {
        this.port = port;
    }

    public MQUDPServer(int port, boolean isPushMode) {
        this.port = port;
        this.isPushMode = isPushMode;
    }


    @Override
    public void start(ChannelInboundHandlerAdapter handler) {
        workGroup = new NioEventLoopGroup(16);
        Bootstrap serverBootstrap = new Bootstrap();
        serverBootstrap.group(workGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel ch) throws Exception {
                        // 加入一个Decoder
                        ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                        ch.pipeline().addLast(new ProtobufEncoder());
                        if (handler != null) {
                            ch.pipeline().addLast(handler);
                        }
                    }
                })
                .option(ChannelOption.SO_BROADCAST, true)
                .option(ChannelOption.SO_RCVBUF, 2048 * 1024)// 设置UDP读缓冲区为2M
                .option(ChannelOption.SO_SNDBUF, 1024 * 1024);// 设置UDP写缓冲区为1M;

        try {
            serverBootstrap.bind(port).sync().channel().closeFuture().await();
            log.info("mq server start at : {} ", port);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (null != workGroup) {
                this.workGroup.shutdownGracefully();
            }
        }
    }

    public void stop() {
        if (null != workGroup) {
            workGroup.shutdownGracefully();
        }
    }
}

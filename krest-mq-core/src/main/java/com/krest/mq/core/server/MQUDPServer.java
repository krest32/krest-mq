package com.krest.mq.core.server;

import com.krest.mq.core.config.MQBuilderConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MqUdpServerHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
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
    MQBuilderConfig mqConfig;

    private MQUDPServer() {
    }

    public MQUDPServer(MQBuilderConfig mqConfig) {
        this.mqConfig = mqConfig;
    }


    @Override
    public void start() {
        workGroup = new NioEventLoopGroup();
        Bootstrap serverBootstrap = new Bootstrap();
        serverBootstrap.group(workGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel ch) throws Exception {
                        // 加入一个Decoder
                        ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                        ch.pipeline().addLast(new ProtobufEncoder());
                        ch.pipeline().addLast(new MqUdpServerHandler());
                    }
                })
                .option(ChannelOption.SO_BROADCAST, true)
                .option(ChannelOption.SO_RCVBUF, 2048 * 1024)// 设置UDP读缓冲区为2M
                .option(ChannelOption.SO_SNDBUF, 1024 * 1024);// 设置UDP写缓冲区为1M;

        try {
            ChannelFuture future = serverBootstrap.bind(mqConfig.getPort()).sync();
            log.info("mq server start at : {} ", mqConfig.getPort());
            future.channel().closeFuture().await();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (null != workGroup) {
                this.workGroup.shutdownGracefully();
            }
        }
    }

}

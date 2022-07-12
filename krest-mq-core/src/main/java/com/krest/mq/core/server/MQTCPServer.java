package com.krest.mq.core.server;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MQTCPServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTCPServer implements MQServer {
    NioEventLoopGroup bossGroup;
    NioEventLoopGroup workGroup;
    boolean isPushMode = false;
    int port;

    private MQTCPServer() {
    }

    public MQTCPServer(@NonNull int port) {
        this.port = port;
    }

    public MQTCPServer(int port, boolean isPushMode) {
        this.port = port;
        this.isPushMode = isPushMode;
    }

    @Override
    public void start(ChannelInboundHandlerAdapter handler) {
        bossGroup = new NioEventLoopGroup();
        workGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(port)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // 加入一个Decoder
                        ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                        ch.pipeline().addLast(new ProtobufEncoder());
                        if (handler != null) {
                            ch.pipeline().addLast(new MQTCPServerHandler(isPushMode));
                        }
                    }
                });

        try {
            ChannelFuture future = serverBootstrap.bind().sync();
            log.info("mq server start at : {} ", port);
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (null != bossGroup) {
                this.bossGroup.shutdownGracefully();
            }
            if (null != workGroup) {
                this.workGroup.shutdownGracefully();
            }
        }
    }

    public void stop() {
        if (null != bossGroup) {
            bossGroup.shutdownGracefully();

        }
        if (null != workGroup) {
            workGroup.shutdownGracefully();
        }
    }
}

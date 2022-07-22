package com.krest.mq.core.server;

import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MqUdpServerHandler;
import com.krest.mq.core.utils.UdpMsgSendUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class MQUDPServer {
    NioEventLoopGroup workGroup;
    public Channel channel;
    Integer port;

    private MQUDPServer() {
    }

    public MQUDPServer(Integer port) {
        this.port = port;
    }


    public void start() {
        workGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workGroup)
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
            ChannelFuture future = bootstrap.bind(this.port).sync();
            log.info("mq udp server start at : {} ", this.port);
            this.channel = future.channel();
            // 发送 broker 信息
            syncBrokerInfo();
            this.channel.closeFuture().await();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (null != workGroup) {
                this.workGroup.shutdownGracefully();
            }
        }
    }

    public void sendMsg(String address, Integer port, MQMessage.MQEntity mqEntity) {
        UdpMsgSendUtils.sendAckMsg(this.channel, address, port, mqEntity);
    }


    private void syncBrokerInfo() {
        // 发送广播信息
//        if (NameServerCache.runningMode.equals(RunningMode.Cluster)) {
//            for (int i = 0; i < NameServerCache.servers.size(); i++) {
//                // 通过 udp 的方式进行链接
//                MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
//                        .setId(UUID.randomUUID().toString())
//                        .setMsg(NameServerCache.servers.get(i)).build();
//                String[] info = NameServerCache.servers.get(i).split(":");
//                String host = info[0];
//                Integer port = Integer.valueOf(info[1]);
//                UdpMsgSendUtils.sendAckMsg(this.channel, host, port, mqEntity);
//            }
//        }
    }
}

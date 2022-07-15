package com.krest.mq.core.handler;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.utils.MsgResolver;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqUdpClientHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private ChannelListener inactiveListener = null;

    public MqUdpClientHandler() {
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        System.out.println("UDP客户端收到消息：");
        MQMessage.MQEntity mqEntity = MsgResolver.parseUdpDatagramPacket(packet);
        System.out.println(mqEntity.getMsg());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("收到异常");
    }

}

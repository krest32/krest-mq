package com.krest.mq.core.handler;

import com.krest.mq.core.processor.UdpServerMsgProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqUdpServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        UdpServerMsgProcessor.msgCenter(ctx, packet);
    }

//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        System.out.println("收到异常");
//        System.out.println(cause);
//    }
}

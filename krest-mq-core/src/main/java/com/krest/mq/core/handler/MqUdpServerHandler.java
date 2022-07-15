package com.krest.mq.core.handler;

import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.processor.TcpMsgProcessor;
import com.krest.mq.core.processor.UdpMsgProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class MqUdpServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    public MqUdpServerHandler() {
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        UdpMsgProcessor.msgCenter(ctx, packet);
    }

}

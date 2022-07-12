package com.krest.mq.core.handler;

import com.krest.mq.core.listener.ChannelInactiveListener;
import com.krest.mq.core.entity.MQMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQUDPClientHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private ChannelInactiveListener inactiveListener = null;

    public MQUDPClientHandler() {
    }

    public MQUDPClientHandler(ChannelInactiveListener channelInactiveListener) {
        this.inactiveListener = channelInactiveListener;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        System.out.println("收到消息");
        ByteBuf buf = packet.copy().content();
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        MQMessage.MQEntity request = MQMessage.MQEntity.parseFrom(req);
        System.out.println(request);
    }
}

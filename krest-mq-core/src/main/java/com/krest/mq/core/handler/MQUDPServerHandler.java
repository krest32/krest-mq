package com.krest.mq.core.handler;

import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.DateUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.*;

@Slf4j
public class MQUDPServerHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    boolean isPushMode = false;

    private MQUDPServerHandler() {
    }

    public MQUDPServerHandler(boolean isPushMode) {
        this.isPushMode = isPushMode;
    }

    static Map<String, Queue<MQMessage.MQEntity>> queueMap = new HashMap<>();

    static Map<String, Channel> ctxMap = new HashMap<>();

    static ChannelGroup clientChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
        log.info("收到消息");
        ByteBuf buf = packet.copy().content();
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        MQMessage.MQEntity request = MQMessage.MQEntity.parseFrom(req);
        System.out.println(request);

        MQMessage.MQEntity response = MQMessage.MQEntity.newBuilder()
                .setId(request.getId())
                .setAck(true)
                .setMsg("回复")
                .setDateTime(DateUtils.getNowDate())
                .build();

        System.out.println(response);
        DatagramPacket responseData = new DatagramPacket(Unpooled.copiedBuffer(response.toByteArray()), packet.sender());
        ctx.writeAndFlush(responseData);
    }


}

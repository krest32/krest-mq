package com.krest.mq.core.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
public class MsgResolver {

    /**
     * 解析 packet 中的信息
     */
    public static MQMessage.MQEntity parseUdpDatagramPacket(DatagramPacket packet) {
        ByteBuf buf = packet.copy().content();
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        MQMessage.MQEntity mqEntity = null;
        try {
            mqEntity = MQMessage.MQEntity.parseFrom(req);
        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage());
        }
        return mqEntity;
    }

    /**
     * 构建返回对象信息
     */
    public static DatagramPacket buildUdpDatagramPacket(MQMessage.MQEntity mqEntity, InetSocketAddress socket) {
        DatagramPacket responseData = new DatagramPacket(
                Unpooled.copiedBuffer(mqEntity.toByteArray()), socket);

        return responseData;
    }

}

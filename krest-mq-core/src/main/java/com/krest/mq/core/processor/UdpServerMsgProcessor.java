package com.krest.mq.core.processor;

import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.MsgResolver;
import com.krest.mq.core.utils.DateUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;


/**
 * 消息处理中心
 */
@Slf4j
public class UdpServerMsgProcessor {

    /**
     * udp server 的作用用来数据备份传输，所以要加入应答机制，避免出现消息丢失的情况
     */
    public static void msgCenter(ChannelHandlerContext ctx, DatagramPacket datagramPacket) {
        MQMessage.MQEntity entity = MsgResolver.parseUdpDatagramPacket(datagramPacket);
        // 设置 udp channel， 用于发送数据信息
        if (null == BrokerLocalCache.udpChannel) {
            BrokerLocalCache.udpChannel = ctx.channel();
        }

        // 开始根据消息类型处理消息 1. 生产者  2. 消费者  3. 回复类型消息
        switch (entity.getMsgType()) {
            case 1:
                producer(ctx, entity, datagramPacket);
                break;
            case 2:
                consumer(ctx, entity, datagramPacket);
                break;
            case 3:
                // 用来处理回复类型的信息
                handlerAckMsg(entity);
                break;
            default:
                log.error("unknown msg type");
                break;
        }
    }


    private static void consumer(ChannelHandlerContext ctx, MQMessage.MQEntity entity, DatagramPacket datagramPacket) {
        MsgResolver.handleConsumerMsg(ctx, entity);
        returnAckMsg(ctx, entity, datagramPacket);
    }


    private static void producer(ChannelHandlerContext ctx, MQMessage.MQEntity entity, DatagramPacket datagramPacket) {
        MsgResolver.handlerProducerMsg(entity);
        returnAckMsg(ctx, entity, datagramPacket);
    }

    /**
     * 回复 ack 的信息
     */
    private static void returnAckMsg(ChannelHandlerContext ctx, MQMessage.MQEntity entity, DatagramPacket datagramPacket) {
        MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
                .setId(entity.getId())
                .setMsg("ack return")
                .setMsgType(3)
                .build();
        DatagramPacket responseData = new DatagramPacket(
                Unpooled.copiedBuffer(mqEntity.toByteArray()), datagramPacket.sender());
        ctx.writeAndFlush(responseData);
    }

    // 添加到回复的队列当中
    private static void handlerAckMsg(MQMessage.MQEntity entity) {
        AdminServerCache.responseQueue.add(entity);
    }


    private static void handlerErr(ChannelHandlerContext ctx, MQMessage.MQEntity entity, String errorMsg) {
        MQMessage.MQEntity response = MQMessage.MQEntity.newBuilder().setId(entity.getId())
                .setErrFlag(true)
                .setMsg(errorMsg)
                .setDateTime(DateUtils.getNowDate())
                .build();
        System.out.println(response);
    }
}


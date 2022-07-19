package com.krest.mq.core.processor;

import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.cache.NameServerCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.runnable.UdpSendMsgRunnable;
import com.krest.mq.core.utils.MsgResolver;
import com.krest.mq.core.exeutor.ExecutorFactory;
import com.krest.mq.core.exeutor.ThreadPoolConfig;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.UdpMsgSendUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;


/**
 * 消息处理中心
 */
@Slf4j
public class UdpServerMsgProcessor {

    /**
     * udp server 的作用用来数据备份传输，所以要加入应答机制
     */
    public static void msgCenter(ChannelHandlerContext ctx, DatagramPacket datagramPacket) {
        MQMessage.MQEntity entity = MsgResolver.parseUdpDatagramPacket(datagramPacket);

        // 设置 udp channel， 用于发送数据信息
        if (null == BrokerLocalCache.udpChannel) {
            BrokerLocalCache.udpChannel = ctx.channel();
        }
        // 开始根据消息类型处理消息
        if (entity.getMsgType() == 3) {
            handlerAckMsg(entity);
        } else {
            // 凡不是回复类型的消息，都需要进行ack确认
            returnAckMsg(ctx, entity, datagramPacket);
        }
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
        NameServerCache.responseQueue.add(entity);
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


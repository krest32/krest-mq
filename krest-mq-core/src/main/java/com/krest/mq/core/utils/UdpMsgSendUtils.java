package com.krest.mq.core.utils;


import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

@Slf4j
public class UdpMsgSendUtils {




    public static boolean sendAckMsg(Channel channel, String host, Integer port, MQMessage.MQEntity mqEntity) {
        int tryCnt = 0;
        while (tryCnt < 3) {
            InetSocketAddress socket = new InetSocketAddress(host, port);
            DatagramPacket responseData = new DatagramPacket(
                    Unpooled.copiedBuffer(mqEntity.toByteArray()), socket);
            if (doSendAckMsg(channel, mqEntity, responseData)) {
                return true;
            } else {
                tryCnt++;
                log.info("retry : {} ", tryCnt);
            }
        }
        return false;
    }

    public static boolean sendAckMsg(Channel channel, DatagramPacket packet, MQMessage.MQEntity mqEntity) {
        int tryCnt = 0;
        while (tryCnt < 3) {
            DatagramPacket responseData = new DatagramPacket(
                    Unpooled.copiedBuffer(mqEntity.toByteArray()), packet.sender());
            if (doSendAckMsg(channel, mqEntity, responseData)) {
                return true;
            } else {
                tryCnt++;
                log.info("retry : {} ", tryCnt);
            }
        }
        return false;
    }

    private static boolean doSendAckMsg(Channel channel, MQMessage.MQEntity mqEntity, DatagramPacket responseData) {
        try {
            // 设置一秒的反馈时间
            MQRespFuture future = new MQRespFuture(mqEntity.getId(), 1000);
            AdminServerCache.udpRespFutureHandler.register(mqEntity.getId(), future);
            channel.writeAndFlush(responseData);
            if (future.isSuccess()) {
                return true;
            }
        } catch (Exception e) {
            log.error("error");
        }
        return false;
    }

}

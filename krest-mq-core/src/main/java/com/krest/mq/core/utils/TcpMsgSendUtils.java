package com.krest.mq.core.utils;

import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class TcpMsgSendUtils {

    public static boolean normalSendMode(Channel channel, MQMessage.MQEntity mqEntity) {
        channel.writeAndFlush(mqEntity);
        return true;
    }


    public static Channel randomChannel(List<Channel> channels) {
        return channels.get(new Random().nextInt(channels.size()));
    }

    /**
     * ack机制： 发送失败后，会进行重试，但问题是它会阻塞在这里，造成性能瓶颈
     */
    public static boolean ackSendMode(Channel channel, MQMessage.MQEntity mqEntity) {
        int tryCnt = 0;
        while (tryCnt < 3) {
            try {
                MQRespFuture respFuture = new MQRespFuture(mqEntity.getId(), 0);
                BrokerLocalCache.tcpRespFutureHandler.register(mqEntity.getId(), respFuture);
                channel.writeAndFlush(mqEntity);
                if (respFuture.getTimeout() == 0) {
                    respFuture.get();
                } else {
                    respFuture.get(respFuture.getTimeout());
                }
                return true;
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
            tryCnt++;
        }
        return false;
    }
}

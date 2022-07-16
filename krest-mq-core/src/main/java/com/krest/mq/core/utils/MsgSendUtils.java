package com.krest.mq.core.utils;

import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@Slf4j
public class MsgSendUtils {

    public static boolean normalSendMode(Channel channel, MQMessage.MQEntity mqEntity) throws ExecutionException, InterruptedException {
        int tryCount = 0;
        while (tryCount < 3) {
            ChannelFuture future = channel.writeAndFlush(mqEntity);
            future.get();
            if (future.isSuccess()) {
                return true;
            }
            tryCount++;
        }
        return false;
    }


    public static Channel randomChannel(List<Channel> channels) {
        return channels.get(new Random().nextInt(channels.size()));
    }

    /**
     * ack机制： 发送失败后，会进行重试
     */
    public static boolean ackSendMode(Channel channel, MQMessage.MQEntity mqEntity) {
        int tryCnt = 0;
        while (tryCnt < 3) {
            try {
                MQRespFuture respFuture = new MQRespFuture(mqEntity.getId(), 0);
                LocalCache.respFutureHandler.register(mqEntity.getId(), respFuture);
                channel.writeAndFlush(mqEntity);
                if (respFuture.getTimeout() == 0) {
                    respFuture.get();
                } else {
                    respFuture.get(respFuture.getTimeout());
                }
                return true;
                // 发送失败就进行重试
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } catch (Throwable throwable) {
                log.error(throwable.getMessage(), throwable);
            }
            tryCnt++;
        }
        return false;
    }
}
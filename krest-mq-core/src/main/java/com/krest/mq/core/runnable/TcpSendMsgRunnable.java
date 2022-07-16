package com.krest.mq.core.runnable;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import com.krest.mq.core.utils.MsgSendUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

@Slf4j
public class TcpSendMsgRunnable implements Runnable {

    String queueName;

    public TcpSendMsgRunnable(@NonNull String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void run() {
        while (true) {
            MQMessage.MQEntity mqEntity = null;
            try {
                mqEntity = LocalCache.queueMap.get(queueName).takeFirst();
                List<Channel> channels = LocalCache.queueCtxListMap.get(queueName);
                if (null != channels && channels.size() > 0) {
                    // 开始发送
                    if (sendMsg(mqEntity, channels)) {
                        // 更新本地的缓存的偏移量
                        LocalCache.queueInfoMap.get(queueName).setOffset(mqEntity.getId());
                        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, LocalCache.queueInfoMap);
                    } else {
                        LocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    }
                } else {
                    log.info("不存在客户端, 等待15s, 继续执行");
                    LocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    Thread.sleep(15 * 1000);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.getMessage(), e);
                // 如果在去除消息后发生了异常，仍然需要吧消息返回队列
                if (null != mqEntity) {
                    try {
                        LocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    } catch (InterruptedException interruptedException) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    private boolean sendMsg(MQMessage.MQEntity mqEntity, List<Channel> channels) throws ExecutionException, InterruptedException {
        boolean flag = true;
        // 如果是单点发送
        if (mqEntity.getTransferType() == 1) {
            Channel channel = MsgSendUtils.randomChannel(channels);
            if (mqEntity.getIsAck()) {
                flag = MsgSendUtils.ackSendMode(channel, mqEntity);
            } else {
                flag = MsgSendUtils.normalSendMode(channel, mqEntity);
            }
            // 如果是广播发送
        } else {
            for (Channel channel : channels) {
                if (mqEntity.getIsAck()) {
                    // ack 机制判断发送模式
                    flag = MsgSendUtils.ackSendMode(channel, mqEntity);
                } else {
                    flag = MsgSendUtils.normalSendMode(channel, mqEntity);
                    if (!flag) {
                        return false;
                    }
                }
            }
        }
        return flag;
    }
}

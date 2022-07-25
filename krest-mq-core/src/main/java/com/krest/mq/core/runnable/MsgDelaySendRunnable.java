package com.krest.mq.core.runnable;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.TcpMsgSendUtils;
import io.netty.channel.Channel;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * tcp 延时队列结果处理
 */
@Slf4j
public class MsgDelaySendRunnable implements Runnable {

    String queueName;

    public MsgDelaySendRunnable(@NonNull String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void run() {
        while (true) {
            DelayMessage delayMessage = null;
            try {
                delayMessage = BrokerLocalCache.delayQueueMap.get(queueName).take();
                List<Channel> channels = BrokerLocalCache.queueCtxListMap.get(queueName);
                if (null != channels && channels.size() > 0) {
                    // 开始发送
                    if (sendMsg(delayMessage.getMqEntity(), channels)) {
                        // 更新本地的缓存的偏移量
                        BrokerLocalCache.queueInfoMap.get(queueName).setOffset(delayMessage.getMqEntity().getId());
                        BrokerLocalCache.queueInfoMap.get(queueName).setAmount(BrokerLocalCache.delayQueueMap.get(queueName).size());
                        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, BrokerLocalCache.queueInfoMap);
                    } else {
                        BrokerLocalCache.delayQueueMap.get(queueName).put(delayMessage);
                    }
                } else {
                    log.info("不存在客户端, 等待15s, 继续执行");
                    BrokerLocalCache.delayQueueMap.get(queueName).put(delayMessage);
                    Thread.sleep(15 * 1000);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.getMessage(), e);
                // 如果在去除消息后发生了异常，仍然需要吧消息返回队列
                if (null != delayMessage) {
                    BrokerLocalCache.delayQueueMap.get(queueName).put(delayMessage);
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean sendMsg(MQMessage.MQEntity mqEntity, List<Channel> channels) throws ExecutionException, InterruptedException, TimeoutException {
        boolean flag = true;
        // 如果是单点发送
        if (mqEntity.getTransferType() == 1) {
            Channel channel = randomChannel(channels);
            if (mqEntity.getIsAck()) {
                flag = TcpMsgSendUtils.ackSendMode(channel, mqEntity);
            } else {
                flag = TcpMsgSendUtils.normalSendMode(channel, mqEntity);
            }
            // 如果是广播发送
        } else {
            for (Channel channel : channels) {
                if (mqEntity.getIsAck()) {
                    // ack 机制判断发送模式
                    flag = TcpMsgSendUtils.ackSendMode(channel, mqEntity);
                } else {
                    flag = TcpMsgSendUtils.normalSendMode(channel, mqEntity);
                    if (!flag) {
                        return false;
                    }
                }
            }
        }
        return flag;
    }

    private Channel randomChannel(List<Channel> channels) {
        return channels.get(new Random().nextInt(channels.size()));
    }

}

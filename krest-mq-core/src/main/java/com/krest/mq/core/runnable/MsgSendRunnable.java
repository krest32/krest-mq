package com.krest.mq.core.runnable;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.SyncUtil;
import com.krest.mq.core.utils.TcpMsgSendUtils;
import io.netty.channel.Channel;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class MsgSendRunnable implements Runnable {

    String queueName;

    public MsgSendRunnable(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void run() {
        while (true) {
            MQMessage.MQEntity mqEntity = null;
            try {
                mqEntity = BrokerLocalCache.queueMap.get(queueName).take();
                List<Channel> channels = BrokerLocalCache.queueCtxListMap.get(queueName);
                // 开始发送
                if (null != channels && channels.size() > 0) {
                    if (sendMsg(mqEntity, channels)) {
                        // 更新本地的缓存的偏移量
                        SyncUtil.saveQueueInfoMap(queueName, mqEntity.getId());
                        SyncUtil.msgReleaseToOtherSever(queueName, mqEntity);
                    } else {
                        BrokerLocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    }
                } else {
                    log.info("不存在客户端, 等待15s, 继续执行");
                    BrokerLocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    Thread.sleep(15 * 1000);
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
                // 如果在去除消息后发生了异常，仍然需要吧消息返回队列
                if (null != mqEntity) {
                    try {
                        BrokerLocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    } catch (InterruptedException interruptedException) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
    }


    private boolean sendMsg(MQMessage.MQEntity mqEntity, List<Channel> channels) {
        boolean flag = true;
        // 如果是单点发送
        if (mqEntity.getTransferType() == 1) {
            Channel channel = TcpMsgSendUtils.randomChannel(channels);
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
}

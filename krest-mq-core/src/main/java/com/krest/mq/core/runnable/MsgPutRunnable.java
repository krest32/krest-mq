package com.krest.mq.core.runnable;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.utils.SyncUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
public class MsgPutRunnable implements Runnable {

    String queueName;
    MQMessage.MQEntity mqEntity;

    public MsgPutRunnable(String queueName, MQMessage.MQEntity mqEntity) {
        this.queueName = queueName;
        this.mqEntity = mqEntity;
    }

    @Override
    public void run() {
        try {
            QueueInfo queueInfo = BrokerLocalCache.queueInfoMap.get(queueName);
            // 只有是持久化的队列才会持久化信息
            if (queueInfo == null || queueInfo.getType() == null) {
                log.error("未知的 queue 或者 queue type");
            } else {
                if (queueInfo.getType().equals(QueueType.TEMPORARY)) {
                    BrokerLocalCache.queueMap.get(queueName).put(this.mqEntity);

                } else {

                    String print = JsonFormat.printer().print(mqEntity);
                    KrestFileHandler.saveData(CacheFileConfig.queueCacheDatePath + queueName,
                            mqEntity.getId(),
                            JSONObject.toJSONString(print));
                    if (queueInfo.getType().equals(QueueType.DELAY)) {
                        BrokerLocalCache.delayQueueMap.get(queueName).put(
                                new DelayMessage(this.mqEntity.getTimeout(), this.mqEntity));
                    } else {
                        BrokerLocalCache.queueMap.get(this.queueName).put(this.mqEntity);
                    }
                    // 更新本地的缓存的偏移量
                    String offset = BrokerLocalCache.queueInfoMap.get(queueName).getOffset();
                    SyncUtil.saveQueueInfoMap(queueName, offset);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage());
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}

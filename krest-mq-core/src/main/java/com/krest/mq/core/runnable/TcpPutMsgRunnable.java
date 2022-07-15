package com.krest.mq.core.runnable;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.QueueType;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.logging.FileHandler;

@Slf4j
public class TcpPutMsgRunnable implements Runnable {

    String queueName;
    MQMessage.MQEntity mqEntity;

    public TcpPutMsgRunnable(String queueName, MQMessage.MQEntity mqEntity) {
        this.queueName = queueName;
        this.mqEntity = mqEntity;
    }

    @Override
    public void run() {
        try {
            QueueInfo queueInfo = LocalCache.queueInfoMap.get(queueName);
            // 只有是持久化的队列才会持久化信息
            if (null != queueInfo.getType() && QueueType.PERMANENT.equals(queueInfo.getType())) {
                String print = JsonFormat.printer().print(mqEntity);
                KrestFileHandler.saveData(CacheFileConfig.queueCacheDatePath + queueName,
                        mqEntity.getId(),
                        JSONObject.toJSONString(print));
            }
            // 如果队列已经满了，当前程序会阻塞在这里
            LocalCache.queueMap.get(queueName).put(this.mqEntity);
        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage());
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}

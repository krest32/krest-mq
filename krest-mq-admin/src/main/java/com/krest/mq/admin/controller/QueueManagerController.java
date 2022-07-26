package com.krest.mq.admin.controller;

import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.enums.QueueType;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

@RestController
@RequestMapping("queue/manager")
public class QueueManagerController {


    /**
     * 获取普通队列中数据的情况
     */
    @GetMapping("get/normal/queue/info/{queueName}")
    public Integer getNormalQueue(@PathVariable String queueName) {
        BlockingDeque<MQMessage.MQEntity> blockingDeque = BrokerLocalCache.queueMap.get(queueName);
        return blockingDeque.size();
    }

    /**
     * 获取普通队列中数据的情况
     */
    @GetMapping("get/normal/queue/empty/{queueName}")
    public boolean isNormalQueueEmpty(@PathVariable String queueName) {
        return BrokerLocalCache.queueMap.get(queueName).isEmpty();
    }

    /**
     * 获取延时队列中数据的情况
     */
    @GetMapping("get/delay/queue/info/{queueName}")
    public Integer getDelayQueue(@PathVariable String queueName) {
        if (BrokerLocalCache.delayQueueMap.get(queueName) != null) {
            return BrokerLocalCache.delayQueueMap.get(queueName).size();
        }
        return -1;
    }


    /**
     * 获取所有队列的 基本信息
     */
    @GetMapping("get/base/queue/info")
    public ConcurrentHashMap<String, QueueInfo> getQueueInfo() {
        // 遍历当前 server 的 queue info map ， 同时为其添加 kid 信息
        for (Map.Entry<String, QueueInfo> entry : BrokerLocalCache.queueInfoMap.entrySet()) {
            if (StringUtils.isBlank(entry.getValue().getKid())) {
                entry.getValue().setKid(AdminServerCache.kid);
            }

            // 更新 offset size
            QueueInfo queueInfo = entry.getValue();
            if (queueInfo.getType().equals(QueueType.DELAY)) {
                DelayQueue<DelayMessage> delayQueue = BrokerLocalCache.delayQueueMap.get(queueInfo.getName());
                if (null != delayQueue && delayQueue.size() > 0) {
                    queueInfo.setOffset(delayQueue.peek().getMqEntity().getId());
                }
                queueInfo.setAmount(null == delayQueue ? -1 : delayQueue.size());
            } else {
                BlockingDeque<MQMessage.MQEntity> blockingDeque = BrokerLocalCache.queueMap.get(queueInfo.getName());
                if (null != blockingDeque && blockingDeque.size() > 0) {
                    queueInfo.setOffset(blockingDeque.peek().getId());
                }
                queueInfo.setAmount(null == blockingDeque ? -1 : blockingDeque.size());
            }


            entry.setValue(queueInfo);
        }
        return BrokerLocalCache.queueInfoMap;
    }

    @GetMapping("sync/data")
    public void syncQueueData() {
        SyncDataUtils.syncClusterInfo();
    }
}

package com.krest.mq.admin.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.mq.admin.util.SyncDataUtil;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.SyncInfo;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.utils.SyncUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.DelayQueue;


@Slf4j
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
    public Map<String, QueueInfo> getQueueInfo() {
        // 遍历当前 server 的 queue info map ， 同时为其添加 kid 信息
        for (Map.Entry<String, QueueInfo> entry : BrokerLocalCache.queueInfoMap.entrySet()) {
            if (StringUtils.isBlank(entry.getValue().getKid())) {
                entry.getValue().setKid(AdminServerCache.kid);
            }
            // 更新 size
            // 更新 offset
            QueueInfo queueInfo = entry.getValue();
            if (queueInfo.getType().equals(QueueType.DELAY)) {
                DelayQueue<DelayMessage> delayQueue = BrokerLocalCache.delayQueueMap.get(queueInfo.getName());
                queueInfo.setAmount(null == delayQueue ? -1 : delayQueue.size());
            } else {
                BlockingDeque<MQMessage.MQEntity> blockingDeque = BrokerLocalCache.queueMap.get(queueInfo.getName());
                queueInfo.setAmount(null == blockingDeque ? -1 : blockingDeque.size());
            }

            entry.setValue(queueInfo);
        }
        return BrokerLocalCache.queueInfoMap;
    }

    @GetMapping("sync/data")
    public void syncQueueData() {
        SyncDataUtil.syncClusterInfo();
    }


    /**
     * 客户端(消费者)请求得到 netty 的远程地址
     */
    @PostMapping("release/msg")
    public void getServerInfo(@RequestBody String reqStr) throws InvalidProtocolBufferException {
        SyncInfo syncInfo = JSONObject.parseObject(reqStr, SyncInfo.class);
        String mqEntityStr = syncInfo.getMqEntityStr();
        MQMessage.MQEntity.Builder tempBuilder = MQMessage.MQEntity.newBuilder();
        JsonFormat.parser().merge(mqEntityStr, tempBuilder);
        MQMessage.MQEntity mqEntity = tempBuilder.build();

        BlockingDeque<MQMessage.MQEntity> normalQueue = BrokerLocalCache.queueMap.get(syncInfo.getQueueName());
        if (normalQueue != null) {
            normalQueue.remove(mqEntity);
            SyncUtil.saveQueueInfoMap(syncInfo.getQueueName(), mqEntity.getId());
        }
        DelayQueue<DelayMessage> delayMessages = BrokerLocalCache.delayQueueMap.get(syncInfo.getQueueName());
        if (delayMessages != null) {
            delayMessages.remove(mqEntity);
            SyncUtil.saveQueueInfoMap(syncInfo.getQueueName(), mqEntity.getId());
        }
    }


    @GetMapping("check/in/use")
    public String checkIsInUSe() {
        // 为空 返回 -1， 否则返回 1
        return BrokerLocalCache.clientChannels.isEmpty() ? "-1" : "1";
    }


}

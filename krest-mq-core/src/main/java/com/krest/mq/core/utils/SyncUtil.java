package com.krest.mq.core.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.entity.SyncInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Set;


@Slf4j
public class SyncUtil {

    static String releaseMsg = "/queue/manager/release/msg";

    public static void saveQueueInfoMap(String queueName, String offset) {
        BrokerLocalCache.queueInfoMap.get(queueName).setOffset(offset);
        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, BrokerLocalCache.queueInfoMap);
    }

    /**
     * 通过 udp 的方式，发送消息到 其他 server
     */
    public static void msgToOtherServer(MQMessage.MQEntity mqEntity) {
        ProtocolStringList queueList = mqEntity.getQueueList();
        for (String queueName : queueList) {
            Set<ServerInfo> serverInfoSet = AdminServerCache.clusterInfo.get().getQueuePacketMap().get(queueName);
            if (null != serverInfoSet
                    && null != AdminServerCache.mqudpServer
                    && serverInfoSet.size() > 0) {
                Iterator<ServerInfo> iterator = serverInfoSet.iterator();
                while (iterator.hasNext()) {
                    ServerInfo serverInfo = iterator.next();
                    if (!serverInfo.getKid().equals(AdminServerCache.kid)) {
                        AdminServerCache.mqudpServer.sendMsg(serverInfo.getAddress(), serverInfo.getUdpPort(), mqEntity);
                    }
                }
            }
        }
    }

    /**
     * 释放消息到其他 sever
     */
    public static void msgReleaseToOtherSever(String queueName, MQMessage.MQEntity mqEntity) {
        Set<ServerInfo> serverInfoSet = AdminServerCache.clusterInfo.get().getQueuePacketMap().get(queueName);
        if (null != serverInfoSet && !serverInfoSet.isEmpty()) {
            for (ServerInfo serverInfo : serverInfoSet) {
                if (!serverInfo.getKid().equals(AdminServerCache.kid)) {
                    String targetUrl = "http://" + serverInfo.getTargetAddress() + releaseMsg;
                    SyncInfo syncInfo = new SyncInfo();
                    try {
                        syncInfo.setMqEntityStr(JsonFormat.printer().print(mqEntity));
                    } catch (InvalidProtocolBufferException e) {
                        log.error(e.getMessage(), e);
                    }
                    syncInfo.setQueueName(queueName);
                    MqRequest request = new MqRequest(targetUrl, syncInfo);
                    HttpUtil.postRequest(request);
                }
            }
        }
    }
}

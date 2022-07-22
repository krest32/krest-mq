package com.krest.mq.admin.util;

import com.alibaba.fastjson.JSONObject;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.entity.SynchInfo;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SynchUtils {

    public static MqConfig mqConfig;
    static String getQueueInfoPath = "/mq/manager/get/queue/info";
    static String synchClusterInfoPath = "/mq/manager/synch/cluster/info";
    static String synchQueueDataPath = "/mq/manager/synch/queue/data";
    static String clearBrokerDataPath = "/mq/manager/clear/overdue/data";

    public static void collectQueueInfo() {

        // 遍历所有的 Server 发送请求
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            // 成为 Leader 后， 收集各个节点的 queue info 信息
            synchQueueInfoMap();

            // 删除 kid 上面过期的 queue 的信息
            ConcurrentHashMap<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfoMap = AdminServerCache.clusterInfo.getKidQueueInfo();
            Iterator<Map.Entry<String, ConcurrentHashMap<String, QueueInfo>>> iterator = kidQueueInfoMap.entrySet().iterator();


            while (iterator.hasNext()) {

                Map.Entry<String, ConcurrentHashMap<String, QueueInfo>> next = iterator.next();
                String kid = next.getKey();
                ConcurrentHashMap<String, QueueInfo> queueInfoMap = next.getValue();
                Iterator<Map.Entry<String, QueueInfo>> queueInfoMapIt = queueInfoMap.entrySet().iterator();


                Map<String, Integer> readyQueueInfoMap = new HashMap<>();
                List<String> queueNameList = new ArrayList<>();

                // 检查 kid 上面的 queue 信息是否是最新的， 如果不是就删除
                clearOverdueData(kid, readyQueueInfoMap, queueNameList, queueInfoMapIt);

                // 开始同步最新的数据
                Iterator<Map.Entry<String, Integer>> readyIt = readyQueueInfoMap.entrySet().iterator();
                while (readyIt.hasNext()) {
                    Map.Entry<String, Integer> entry = readyIt.next();

                    SynchInfo synchInfo = new SynchInfo();
                    synchInfo.setType(entry.getValue());
                    synchInfo.setQueueName(entry.getKey());
                    synchInfo.setAddress(AdminServerCache.kidServerMap.get(kid).getAddress());
                    synchInfo.setPort(AdminServerCache.kidServerMap.get(kid).getUdpPort());

                    ServerInfo latestQueueServer = null;
                    String latestKid = AdminServerCache.clusterInfo.getQueueLatestKid().get(entry.getKey());
                    for (ServerInfo serverInfo : mqConfig.getServerList()) {
                        if (serverInfo.getKid().equals(latestKid)) {
                            latestQueueServer = serverInfo;
                            break;
                        }
                    }

                    if (latestQueueServer != null) {
                        String targetUrl = "http://" + latestQueueServer.getTargetAddress() + synchQueueDataPath;
                        System.out.println(targetUrl);
                        System.out.println(synchInfo);
                        MqRequest request = new MqRequest(targetUrl, synchInfo);
                        System.out.println(HttpUtil.postRequest(request));
                    }
                }
            }

            // 清空完数据， 收集各个节点的 queue info 信息
            synchQueueInfoMap();


            // 然后发送已经同步的 cluster 信息
            for (ServerInfo curServer : AdminServerCache.curServers) {
                String targetUrl = "http://" + curServer.getTargetAddress() + synchClusterInfoPath;
                MqRequest request = new MqRequest(targetUrl, AdminServerCache.clusterInfo);
                HttpUtil.postRequest(request);
            }
        }
    }

    private static void clearOverdueData(String kid,
                                         Map<String, Integer> readyQueueInfoMap,
                                         List<String> queueNameList,
                                         Iterator<Map.Entry<String, QueueInfo>> queueInfoMapIt) {
        while (queueInfoMapIt.hasNext()) {
            Map.Entry<String, QueueInfo> infoEntry = queueInfoMapIt.next();
            String queueName = infoEntry.getKey();
            Long offset = Long.valueOf(StringUtils.isBlank(infoEntry.getValue().getOffset()) ? "0" : infoEntry.getValue().getOffset());
            if (AdminServerCache.clusterInfo.getQueueOffsetMap().get(queueName) != null) {
                if (offset.compareTo(AdminServerCache.clusterInfo.getQueueOffsetMap().get(queueName)) < 0) {
                    QueueType queueType = infoEntry.getValue().getType();
                    Integer type = 0;
                    switch (queueType) {
                        case PERMANENT:
                            type = 1;
                            break;
                        case TEMPORARY:
                            type = 2;
                            break;
                        case DELAY:
                            type = 3;
                            break;
                    }
                    queueNameList.add(queueName);
                    readyQueueInfoMap.put(queueName, type);
                }
            }
        }

        // 开始删除过期数据
        for (ServerInfo curServer : AdminServerCache.curServers) {
            if (curServer.getKid().equals(kid)) {
                String targetUrl = "http://" + curServer.getTargetAddress() + clearBrokerDataPath;
                MqRequest request = new MqRequest(targetUrl, queueNameList);
                HttpUtil.postRequest(request);
            }
        }
    }


    private static void synchQueueInfoMap() {
        for (ServerInfo curServer : AdminServerCache.curServers) {
            String targetUrl = "http://" + curServer.getTargetAddress() + getQueueInfoPath;
            MqRequest request = new MqRequest(targetUrl, null);
            // queue -> queue info json string
            ConcurrentHashMap<String, JSONObject> queueInfoStrMap = HttpUtil.getQueueInfo(request);
            if (null != queueInfoStrMap) {
                ConcurrentHashMap<String, QueueInfo> queueInfoMap = new ConcurrentHashMap<>();
                // 更新 queue 的最新 offset
                Iterator<Map.Entry<String, JSONObject>> iterator = queueInfoStrMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, JSONObject> mapEntry = iterator.next();
                    QueueInfo tempQueueInfo = mapEntry.getValue().toJavaObject(QueueInfo.class);
                    String queueName = tempQueueInfo.getName();
                    Long queueOffset = Long.valueOf(tempQueueInfo.getOffset() == null ? "-1" : tempQueueInfo.getOffset());
                    Long curOffset = AdminServerCache.clusterInfo.getQueueOffsetMap().getOrDefault(queueName, 0L);
                    if (queueOffset.compareTo(curOffset) >= 0) {
                        AdminServerCache.clusterInfo.getQueueOffsetMap().put(queueName, queueOffset);
                        AdminServerCache.clusterInfo.getQueueLatestKid().put(queueName, curServer.getKid());
                    }
                    queueInfoMap.put(queueName, tempQueueInfo);
                }
                AdminServerCache.clusterInfo.getKidQueueInfo().put(curServer.getKid(), queueInfoMap);
            }
        }
    }
}

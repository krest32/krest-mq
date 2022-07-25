package com.krest.mq.admin.util;

import com.alibaba.fastjson.JSONObject;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class SyncDataUtils {

    public static MqConfig mqConfig;
    static String getQueueInfoPath = "/queue/manager/get/base/queue/info";
    static String syncClusterInfoPath = "/mq/manager/sync/cluster/info";
    static String syncQueueDataPath = "/mq/manager/sync/queue/data";
    static String clearBrokerDataPath = "/mq/manager/clear/overdue/data";

    public static void syncClusterInfo() {

        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            log.info("start sync cluster info and data ...");
            AdminServerCache.isSyncData = true;
            ClusterInfo clusterInfo = new ClusterInfo();
            clusterInfo.setDuplicate(AdminServerCache.clusterInfo.getDuplicate());
            clusterInfo.setQueueOffsetMap(AdminServerCache.clusterInfo.getQueueOffsetMap());
            clusterInfo.setQueueSizeMap(AdminServerCache.clusterInfo.getQueueSizeMap());

            getQueueInfoMap(clusterInfo);

            ConcurrentHashMap<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfoMap = clusterInfo.getKidQueueInfo();
            Iterator<Map.Entry<String, ConcurrentHashMap<String, QueueInfo>>> iterator = kidQueueInfoMap.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, ConcurrentHashMap<String, QueueInfo>> next = iterator.next();
                String kid = next.getKey();
                ConcurrentHashMap<String, QueueInfo> queueInfoMap = next.getValue();
                Iterator<Map.Entry<String, QueueInfo>> queueInfoMapIt = queueInfoMap.entrySet().iterator();
                Map<String, Integer> readyQueueInfoMap = new HashMap<>();
                List<String> queueNameList = new ArrayList<>();

                // 检查 kid 上面的 queue 信息是否是最新的， 如果不是就删除
                // clearOverdueData(kid, readyQueueInfoMap, queueNameList, queueInfoMapIt, clusterInfo);

                // 开始同步最新的数据
                Iterator<Map.Entry<String, Integer>> readyIt = readyQueueInfoMap.entrySet().iterator();
                while (readyIt.hasNext()) {
                    Map.Entry<String, Integer> entry = readyIt.next();
                    SynchInfo synchInfo = new SynchInfo();
                    synchInfo.setType(entry.getValue());
                    synchInfo.setQueueName(entry.getKey());
                    synchInfo.setOffset(String.valueOf(clusterInfo.getQueueOffsetMap().get(entry.getKey())));
                    synchInfo.setAddress(AdminServerCache.kidServerMap.get(kid).getAddress());
                    synchInfo.setPort(AdminServerCache.kidServerMap.get(kid).getUdpPort());

                    ServerInfo latestQueueServer = null;
                    String latestKid = clusterInfo.getQueueLatestKid().get(entry.getKey());
                    for (ServerInfo serverInfo : mqConfig.getServerList()) {
                        if (serverInfo.getKid().equals(latestKid)) {
                            latestQueueServer = serverInfo;
                            break;
                        }
                    }

                    // 设置 当前 queue 数据所以的 最新 server kid 数据信息地址
//                    if (latestQueueServer != null) {
//                        String targetUrl = "http://" + latestQueueServer.getTargetAddress() + syncQueueDataPath;
//                        MqRequest request = new MqRequest(targetUrl, synchInfo);
//                        HttpUtil.postRequest(request);
//                    }
                }
            }

            // 清空完数据， 收集各个节点的 queue info 信息
            getQueueInfoMap(clusterInfo);

            // 然后发送已经同步的 cluster 信息
            for (ServerInfo curServer : AdminServerCache.curServers) {
                String targetUrl = "http://" + curServer.getTargetAddress() + syncClusterInfoPath;
                MqRequest request = new MqRequest(targetUrl, clusterInfo);
                HttpUtil.postRequest(request);
            }

            log.info("sync cluster info and data complete");
            AdminServerCache.isSyncData = false;
        }
    }


    private static void getQueueInfoMap(ClusterInfo clusterInfo) {
        // 遍历所有的当前 server，获取得到 queue info map
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
                    // 如果不存在 offset 就设置 默认值 -1， 表示没有 offset 值
                    Long queueOffset = Long.valueOf(tempQueueInfo.getOffset() == null ? "-1" : tempQueueInfo.getOffset());
                    Long curOffset = clusterInfo.getQueueOffsetMap().getOrDefault(queueName, 0L);
                    Integer temQueueSize = tempQueueInfo.getAmount();
                    Integer curQueueSize = clusterInfo.getQueueSizeMap().getOrDefault(queueName, 0);
                    clusterInfo.getQueueSizeMap().put(queueName, Math.max(temQueueSize, curQueueSize));

                    if (queueOffset.compareTo(curOffset) >= 0 && temQueueSize >= curQueueSize) {
                        clusterInfo.getQueueOffsetMap().put(queueName, queueOffset);
                        clusterInfo.getQueueLatestKid().put(queueName, curServer.getKid());
                    }
                    queueInfoMap.put(queueName, tempQueueInfo);
                }
                clusterInfo.getKidQueueInfo().put(curServer.getKid(), queueInfoMap);
            }
        }
    }

    private static void clearOverdueData(String kid,
                                         Map<String, Integer> readyQueueInfoMap,
                                         List<String> queueNameList,
                                         Iterator<Map.Entry<String, QueueInfo>> queueInfoMapIt,
                                         ClusterInfo clusterInfo) {

        while (queueInfoMapIt.hasNext()) {
            Map.Entry<String, QueueInfo> infoEntry = queueInfoMapIt.next();
            String queueName = infoEntry.getKey();
            Long offset = Long.valueOf(StringUtils.isBlank(infoEntry.getValue().getOffset()) ? "-1" : infoEntry.getValue().getOffset());
            Integer tempAmount = infoEntry.getValue().getAmount();
            Integer maxAmount = clusterInfo.getQueueSizeMap().getOrDefault(queueName, 0);

            if (clusterInfo.getQueueOffsetMap().get(queueName) != null) {
                if (offset.compareTo(clusterInfo.getQueueOffsetMap().get(queueName)) < 0
                        || tempAmount != maxAmount) {
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


    public static boolean isClusterReady() {
        if (AdminServerCache.isSelectServer) {
            log.info("still in select server....");
            return false;
        }
        if (AdminServerCache.isDetectFollower) {
            log.info("still in detect follower....");
            return false;
        }

        if (AdminServerCache.isSyncData) {
            log.info("still in sync data with other broker....");
            return false;
        }
        return true;
    }
}

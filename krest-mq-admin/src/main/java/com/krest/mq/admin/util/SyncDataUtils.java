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

    /**
     * 1. 清空新注册节点的数据
     * 2. 同步 cluster info
     */
    public static void syncClusterInfo() {

        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {

            log.info("start sync cluster info and data ...");
            AdminServerCache.isSyncData = true;
            ClusterInfo clusterInfo = new ClusterInfo();
            clusterInfo.setDuplicate(AdminServerCache.clusterInfo.getDuplicate());
            clusterInfo.setQueueOffsetMap(AdminServerCache.clusterInfo.getQueueOffsetMap());
            clusterInfo.setQueueSizeMap(AdminServerCache.clusterInfo.getQueueSizeMap());


            // 检查 kid 上面的 queue 信息是否是最新的， 如果不是就删除
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

package com.krest.mq.admin.balancer;

import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.ClusterInfo;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BrokerBalancer {

    public synchronized static void run() {

        if (AdminServerCache.isKidBalanced) {
            log.info("still in balance data, please next time");
            return;
        }

        AdminServerCache.isKidBalanced = true;

        Integer duplicate = AdminServerCache.clusterInfo.getDuplicate();
        if (duplicate > AdminServerCache.curServers.size()) {
            log.error("config duplicate is : {} , but mq server number is : {}", duplicate, AdminServerCache.curServers.size());
            // 重设副本数量最大为集群的数量
            duplicate = AdminServerCache.curServers.size();
        }
        // 开始同步数据
        doSyncData(duplicate);

        AdminServerCache.isKidBalanced = false;
    }

    private static void doSyncData(Integer duplicate) {

        // 开始遍历记录的 queue 数量的列表
        Iterator<Map.Entry<String, Integer>> queueAmountIt = AdminServerCache.clusterInfo
                .getQueueAmountMap().entrySet().iterator();

        while (queueAmountIt.hasNext()) {
            Map.Entry<String, Integer> entry = queueAmountIt.next();
            String queueName = entry.getKey();
            Integer count = entry.getValue();
            // 如果当前队列的数量小于副本数，开始复制队列
            if (count < duplicate) {

                // 获取 from kid
                String fromKid = getFromKid(queueName, AdminServerCache.clusterInfo);
                ServerInfo fromServer = AdminServerCache.kidServerMap.get(fromKid);

                String toKid = getToKid(fromKid);

                // 开始发送同步数据的请求
                String targetUrl = "http://" + fromServer.getTargetAddress() + "/mq/manager/sync/all/queue";
                MqRequest request = new MqRequest(targetUrl, toKid);
                String s = HttpUtil.postRequest(request);

            }
            // 更新信息
            SyncDataUtils.syncClusterInfo();
        }
    }

    /**
     * 获取与 from kid 中 queue 相似度最高的 to kid;
     */
    private static String getToKid(String fromKid) {
        ConcurrentHashMap<String, QueueInfo> fromKidQueueInfoMag = AdminServerCache.clusterInfo.getKidQueueInfo().get(fromKid);
        String toKid = null;
        int maxRelate = Integer.MIN_VALUE;


        for (Map.Entry<String, ConcurrentHashMap<String, QueueInfo>> curQueueInfo : AdminServerCache.clusterInfo.getKidQueueInfo().entrySet()) {
            ConcurrentHashMap<String, QueueInfo> queueInfoMap = curQueueInfo.getValue();
            String kid = curQueueInfo.getKey();

            if (kid.equals(fromKid)) {
                continue;
            }

            Iterator<Map.Entry<String, QueueInfo>> iterator = queueInfoMap.entrySet().iterator();
            int cnt = 0;
            while (iterator.hasNext()) {
                Map.Entry<String, QueueInfo> next = iterator.next();
                String queueName = next.getKey();
                if (fromKidQueueInfoMag.containsKey(queueName)) {
                    cnt++;
                }
            }
            if (cnt > maxRelate) {
                toKid = kid;
            }
        }
        return toKid;
    }


    /**
     * 获取包含 queue 的最少 queue amount 的 kid
     */
    private static String getFromKid(String queueName, ClusterInfo clusterInfo) {
        String kid = null;
        Integer queueAmount = Integer.MAX_VALUE;
        Iterator<Map.Entry<String, ConcurrentHashMap<String, QueueInfo>>> iterator = clusterInfo.getKidQueueInfo().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<String, QueueInfo>> next = iterator.next();
            String tempKid = next.getKey();
            ConcurrentHashMap<String, QueueInfo> infoMap = next.getValue();
            if (infoMap.containsKey(queueName) && infoMap.size() < queueAmount) {
                kid = tempKid;
                queueAmount = infoMap.size();
            }
        }
        return kid;
    }
}

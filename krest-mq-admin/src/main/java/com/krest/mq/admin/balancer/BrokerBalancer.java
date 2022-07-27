package com.krest.mq.admin.balancer;

import com.alibaba.fastjson.JSONObject;
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

    static String checkBrokerQueueInfoPath = "/queue/manager/get/base/queue/info";
    static String clearHisDataPath = "/mq/manager/clear/overdue/data";

    public synchronized static void run() {

        if (AdminServerCache.isKidBalanced) {
            log.info("still in balance data, please next time");
            return;
        }

        if (AdminServerCache.curServers.size() == 1) {
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
        for (ServerInfo curServer : AdminServerCache.curServers) {
            if (checkBrokerQueueInfo(curServer)) {
                // 清空原始数据
                clearHisData(curServer);
                // 同步集群的 queue 信息
                SyncDataUtils.syncClusterInfo();
            }
        }
        // 避免直接跳过了清理数据的地方
        SyncDataUtils.syncClusterInfo();


        Iterator<Map.Entry<String, Integer>> secQueueAmountIt = AdminServerCache.clusterInfo
                .getQueueAmountMap().entrySet().iterator();

        while (secQueueAmountIt.hasNext()) {
            Map.Entry<String, Integer> entry = secQueueAmountIt.next();

            String queueName = entry.getKey();
            Integer count = AdminServerCache.clusterInfo.getQueueAmountMap().get(queueName);
            // 第一种同步情况， 直接复制缺失的 queue 到对应的 kid

            if (count < duplicate) {
                // 获取 from kid
                String fromKid = getFromKid(queueName);
                ServerInfo fromServer = AdminServerCache.kidServerMap.get(fromKid);

                String toKid = getToKid(fromKid);

                // 开始发送同步数据的请求
                String targetUrl = "http://" + fromServer.getTargetAddress() + "/mq/manager/sync/all/queue";
                MqRequest request = new MqRequest(targetUrl, toKid);
                String s = HttpUtil.postRequest(request);

                SyncDataUtils.syncClusterInfo();
            }
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

    private static String getFromKid(String queueName) {
        return AdminServerCache.clusterInfo.getQueueLatestKid().get(queueName);
    }


    private static boolean checkBrokerQueueInfo(ServerInfo serverInfo) {
        String targetUrl = "http://" + serverInfo.getTargetAddress() + checkBrokerQueueInfoPath;
        MqRequest request = new MqRequest(targetUrl, null);

        ConcurrentHashMap<String, JSONObject> queueInfoStrMap = HttpUtil.getQueueInfo(request);
        if (null != queueInfoStrMap) {
            // 更新 queue 的最新 offset
            Iterator<Map.Entry<String, JSONObject>> iterator = queueInfoStrMap.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, JSONObject> mapEntry = iterator.next();
                QueueInfo tempQueueInfo = mapEntry.getValue().toJavaObject(QueueInfo.class);

                String queueName = tempQueueInfo.getName();
                // 判断值是否存在，不存在给定默认值
                Long tempOffset = Long.valueOf(
                        tempQueueInfo.getOffset() == null
                                ? "-1L" : tempQueueInfo.getOffset()
                );

                Integer tempSize = tempQueueInfo.getAmount() == null
                        ? -1 : tempQueueInfo.getAmount();

                // 检查 offset 和 amount, 如果 offset 和 amount 有一处不一致，那么就删除
                Long offset = Long.valueOf(
                        AdminServerCache.clusterInfo.getQueueOffsetMap().get(queueName) == null
                                ? -1 : AdminServerCache.clusterInfo.getQueueOffsetMap().get(queueName)
                );
                Integer size = AdminServerCache.clusterInfo.getQueueSizeMap().get(queueName) == null
                        ? -1 : AdminServerCache.clusterInfo.getQueueSizeMap().get(queueName);


                // 如果 集群的偏移量 大于 新注册的 broker, 说明 broker 的数据是旧的， 执行删除
                if (offset.compareTo(tempOffset) > 0) {
                    return true;
                }
                // 如果 偏移量相同，但是新注册的 broker queue size 大于 cluster 中的 queue size，
                // 那就说明 新的 broker 数据更新
                if (offset.compareTo(tempOffset) == 0 && size > tempSize) {
                    return true;
                }

                // 判断 queue 在 cluster 中数量是否超过了 最大副本数
                Integer amount = AdminServerCache.clusterInfo.getQueueAmountMap()
                        .getOrDefault(queueName, 0);
                if (amount > AdminServerCache.clusterInfo.getDuplicate()) {
                    return true;
                }
            }
        }
        return false;
    }


    private static void clearHisData(ServerInfo serverInfo) {
        String targetUrl = "http:\\" + serverInfo.getTargetAddress() + clearHisDataPath;
        MqRequest request = new MqRequest(targetUrl, null);
        HttpUtil.postRequest(request);
    }
}

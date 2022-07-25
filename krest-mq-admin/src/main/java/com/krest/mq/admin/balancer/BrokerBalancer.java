package com.krest.mq.admin.balancer;

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

    public static void run() {

        AdminServerCache.isKidBalanced = false;

        Integer duplicate = AdminServerCache.clusterInfo.getDuplicate();
        if (duplicate > AdminServerCache.curServers.size()) {
            log.error("config duplicate is : {} , but mq server number is : {}", duplicate, AdminServerCache.curServers.size());
            // 重设副本数量最大为集群的数量
            duplicate = AdminServerCache.curServers.size();
        }

        // 初始化数据
        AdminServerCache.clusterInfo.getQueueAmountMap().clear();

        // 记录每个 queue 现有的数量
        ClusterInfo clusterInfo = AdminServerCache.clusterInfo;
        Map<String, Integer> kidQueueAmountMap = new HashMap<>();
        countKidAndQueue(clusterInfo, kidQueueAmountMap);

        // 开始同步数据
        doSyncData(duplicate, clusterInfo, kidQueueAmountMap);

        AdminServerCache.isKidBalanced = true;
    }

    private static void doSyncData(Integer duplicate, ClusterInfo clusterInfo, Map<String, Integer> kidQueueAmountMap) {
        // 进行升序排序： 注意：要有排序的描述条件，否则无法排序
        PriorityQueue<Map.Entry<String, Integer>> sortedQueue = new PriorityQueue<>(
                Comparator.comparingInt(Map.Entry::getValue));

        Iterator<Map.Entry<String, Integer>> iterator = kidQueueAmountMap.entrySet().iterator();
        while (iterator.hasNext()) {
            sortedQueue.add(iterator.next());
        }

        // 开始遍历记录的 queue 数量的列表
        Iterator<Map.Entry<String, Integer>> queueAmountIt =
                clusterInfo.getQueueAmountMap().entrySet().iterator();

        while (queueAmountIt.hasNext()) {
            Map.Entry<String, Integer> entry = queueAmountIt.next();
            String queueName = entry.getKey();
            Integer count = entry.getValue();

            // 如果当前队列的数量小于副本数，开始复制队列
            if (count < duplicate) {
                // 获取 to kid
                Map.Entry<String, Integer> poll = sortedQueue.poll();
                String toKid = poll.getKey();

                // 获取 from kid
                // 因为是节点对节点的复制，所以 from kid 上面的队列数量应该是最少的
                String fromKid = getFromKid(queueName, clusterInfo);
                ServerInfo fromServer = AdminServerCache.kidServerMap.get(fromKid);

                // 开始发送同步数据的请求
                String targetUrl = "http://" + fromServer.getTargetAddress() + "/mq/manager/sync/all/queue";
                MqRequest request = new MqRequest(targetUrl, toKid);
                HttpUtil.postRequest(request);

                // 更新当前 cluster 的数据信息
                updateEntryInfo(fromKid, toKid, entry, clusterInfo, poll);
                sortedQueue.add(poll);
            }
        }
    }


    private static void countKidAndQueue(ClusterInfo clusterInfo, Map<String, Integer> kidQueueAmountMap) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfo = clusterInfo.getKidQueueInfo();
        Iterator<Map.Entry<String, ConcurrentHashMap<String, QueueInfo>>> kidQueueIterator = kidQueueInfo.entrySet().iterator();
        while (kidQueueIterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<String, QueueInfo>> mapEntry = kidQueueIterator.next();
            Iterator<Map.Entry<String, QueueInfo>> queueInfoIterator = mapEntry.getValue().entrySet().iterator();
            int count = 0;
            while (queueInfoIterator.hasNext()) {
                Map.Entry<String, QueueInfo> infoEntry = queueInfoIterator.next();
                QueueInfo queueInfo = infoEntry.getValue();
                String queueName = queueInfo.getName();
                Integer amount = clusterInfo.getQueueAmountMap().getOrDefault(queueName, 0);
                clusterInfo.getQueueAmountMap().put(queueName, amount + 1);
                count++;
            }
            kidQueueAmountMap.put(mapEntry.getKey(), count);
        }
    }

    private static void updateEntryInfo(String fromKid, String toKid,
                                        Map.Entry<String, Integer> entry,
                                        ClusterInfo clusterInfo,
                                        Map.Entry<String, Integer> poll) {
        // 获取 from kid 上面的所有队列
        ConcurrentHashMap<String, QueueInfo> fromMap = clusterInfo.getKidQueueInfo().get(fromKid);
        ConcurrentHashMap<String, QueueInfo> toMap = clusterInfo.getKidQueueInfo().get(toKid);
        Iterator<Map.Entry<String, QueueInfo>> iterator = fromMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> next = iterator.next();
            String queueName = next.getKey();
            if (!toMap.containsKey(queueName)) {
                clusterInfo.getQueueAmountMap().put(queueName, entry.getValue() + 1);
                poll.setValue(poll.getValue() + 1);
            }
        }
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

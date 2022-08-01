package com.krest.mq.admin.balancer;

import com.alibaba.fastjson.JSONObject;
import com.krest.mq.admin.util.SyncDataUtil;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BrokerBalancer {

    private BrokerBalancer() {
    }

    static final String CHECK_BROKER_QUEUE_INFO_PATH = "/queue/manager/get/base/queue/info";
    static final String CLEAR_HIS_DATA_PATH = "/mq/manager/clear/overdue/data";
    static final String HTTP_START_STR = "http://";

    public synchronized static void run() {

        if (AdminServerCache.isKidBalanced) {
            log.info("still in balance data, please next time");
            return;
        }

        if (AdminServerCache.clusterInfo.get().getCurServers().size() == 1) {
            return;
        }

        AdminServerCache.isKidBalanced = true;

        Integer duplicate = AdminServerCache.clusterInfo.get().getDuplicate();
        if (duplicate > AdminServerCache.clusterInfo.get().getCurServers().size()) {
            log.error("config duplicate is : {} , but mq server number is : {}", duplicate,
                    AdminServerCache.clusterInfo.get().getCurServers().size());
            // 重设副本数量最大为集群的数量
            duplicate = AdminServerCache.clusterInfo.get().getCurServers().size();
        }
        // 开始同步数据
        doSyncData(duplicate);

        AdminServerCache.isKidBalanced = false;
    }

    private static void doSyncData(Integer duplicate) {
        // 开始遍历记录的 queue 数量的列表
        for (ServerInfo curServer : AdminServerCache.clusterInfo.get().getCurServers()) {
            if (checkBrokerQueueInfo(curServer)) {
                // 清空原始数据
                clearHisData(curServer);
                // 同步集群的 queue 信息
                SyncDataUtil.syncClusterInfo();
            }
        }
        // 避免直接跳过了清理数据的地方
        SyncDataUtil.syncClusterInfo();
        Map<String, Integer> queueAmountMap = AdminServerCache.clusterInfo.get().getQueueAmountMap();
        Iterator<Map.Entry<String, Integer>> queueAmountIt = queueAmountMap.entrySet().iterator();
        Map<String, String> kidRelationMap = new ConcurrentHashMap<>();

        while (queueAmountIt.hasNext()) {
            Map.Entry<String, Integer> entry = queueAmountIt.next();
            String queueName = entry.getKey();
            Integer count = entry.getValue();
            // 第一种同步情况， 直接复制缺失的 queue 到对应的 kid
            if (count == null && duplicate == null) {
                return;
            }
            if (count < duplicate) {
                String fromKid = getFromKid(queueName);
                if (StringUtils.isBlank(fromKid)) {
                    continue;
                }
                ServerInfo fromServer = AdminServerCache.kidServerMap.get(fromKid);
                String toKid = getToKid(fromKid);

                if (StringUtils.isBlank(toKid)) {
                    continue;
                }

                // 如果当前的 from kid 与 to kid 已经匹配过一次
                if (kidRelationMap.containsKey(fromKid)
                        && kidRelationMap.get(fromKid).equals(toKid)) {
                    continue;
                } else {
                    kidRelationMap.put(fromKid, toKid);
                }

                ServerInfo toKidServerInfo = AdminServerCache.kidServerMap.get(toKid);

                // 检查 to kid 的 server 是否正在同步数据
                String checkStatusTargetUrl = HTTP_START_STR + toKidServerInfo.getTargetAddress() + "/mq/manager/check/status";
                MqRequest checkRequest = new MqRequest(checkStatusTargetUrl, null);
                String checkResponse = HttpUtil.postRequest(checkRequest);

                // 1 代表 to kid server 可以 同步数据， 修改目标 server 的状态为同步数据中
                if ("1".equals(checkResponse)) {

                    // 改变 toKid 的状态为 sync data 中
                    ConcurrentHashMap<String, QueueInfo> queueInfoMap = AdminServerCache.clusterInfo.get().getKidQueueInfo().get(toKid);
                    String changeStatusTargetUrl = HTTP_START_STR + toKidServerInfo.getTargetAddress() + "/mq/manager/change/kid/status";
                    MqRequest request = new MqRequest(changeStatusTargetUrl, queueInfoMap);
                    String changeStatusResp = HttpUtil.postRequest(request);

                    // 修改状态成功， 开始同步数据
                    if ("1".equals(changeStatusResp)) {
                        // 开始发送同步数据的请求
                        String targetUrl = HTTP_START_STR + fromServer.getTargetAddress() + "/mq/manager/sync/all/queue";
                        MqRequest toKidRequest = new MqRequest(targetUrl, toKid);
                        HttpUtil.postRequest(toKidRequest);
                    }
                }
            }
        }
    }

    /**
     * 获取与 from kid 中 queue 相似度最高的 to kid;
     */
    private static String getToKid(String fromKid) {
        ConcurrentHashMap<String, QueueInfo> fromKidQueueInfoMag = AdminServerCache.clusterInfo.get().getKidQueueInfo().get(fromKid);
        String toKid = null;
        int maxRelate = Integer.MIN_VALUE;

        for (Map.Entry<String, ConcurrentHashMap<String, QueueInfo>> curQueueInfo : AdminServerCache.clusterInfo.get().getKidQueueInfo().entrySet()) {
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
        return AdminServerCache.clusterInfo.get().getQueueLatestKid().get(queueName);
    }


    private static boolean checkBrokerQueueInfo(ServerInfo serverInfo) {
        String targetUrl = HTTP_START_STR + serverInfo.getTargetAddress() + CHECK_BROKER_QUEUE_INFO_PATH;
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
                        null == tempQueueInfo.getOffset()
                                ? "-1L" : tempQueueInfo.getOffset()
                );

                Integer tempSize = null == tempQueueInfo.getAmount()
                        ? -1 : tempQueueInfo.getAmount();

                // 检查 offset 和 amount, 如果 offset 和 amount 有一处不一致，那么就删除
                Long offset = Long.valueOf(
                        AdminServerCache.clusterInfo.get().getQueueOffsetMap().get(queueName) == null
                                ? -1 : AdminServerCache.clusterInfo.get().getQueueOffsetMap().get(queueName)
                );
                Integer size = AdminServerCache.clusterInfo.get().getQueueSizeMap().get(queueName) == null
                        ? -1 : AdminServerCache.clusterInfo.get().getQueueSizeMap().get(queueName);


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
                Integer amount = AdminServerCache.clusterInfo.get().getQueueAmountMap()
                        .getOrDefault(queueName, 0);
                if (amount > AdminServerCache.clusterInfo.get().getDuplicate() && !isInUse(serverInfo)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isInUse(ServerInfo serverInfo) {
        String targetUrl = HTTP_START_STR + serverInfo.getTargetAddress() + "/queue/manager/check/in/use";
        MqRequest request = new MqRequest(targetUrl, null);
        String respStr = HttpUtil.isServerInUse(request);
        if (!StringUtils.isBlank(respStr) && "1".equals(respStr)) {
            return true;
        }
        return false;
    }


    private static void clearHisData(ServerInfo serverInfo) {
        String targetUrl = HTTP_START_STR + serverInfo.getTargetAddress() + CLEAR_HIS_DATA_PATH;
        MqRequest request = new MqRequest(targetUrl, null);
        HttpUtil.postRequest(request);
    }
}

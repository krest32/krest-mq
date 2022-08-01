package com.krest.mq.admin.util;

import com.alibaba.fastjson.JSONObject;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class SyncDataUtil {

    private SyncDataUtil() {
    }

    public static MqConfig mqConfig;
    static final String GET_BASE_QUEUE_INFO = "/queue/manager/get/base/queue/info";
    static final String SYNC_CLUSTER_INFO = "/mq/manager/sync/cluster/info";
    static final String CHECK_KID_STATUS = "/mq/manager/check/kid/status";

    /**
     * 1. 清空新注册节点的数据
     * 2. 同步 cluster info
     */
    public synchronized static void syncClusterInfo() {

        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {

            if (AdminServerCache.isSyncClusterInfo) {
                log.info("still in sync cluster info, please wait next run schedule job");
                return;
            }

            AdminServerCache.isSyncClusterInfo = true;
            ClusterInfo clusterInfo = AdminServerCache.clusterInfo.get();

            // 检查 kid 上面的 queue 信息是否是最新的， 如果不是就删除
            geClusterInfo(clusterInfo);

            // 然后发送已经同步的 cluster 信息
            for (ServerInfo curServer : AdminServerCache.clusterInfo.get().getCurServers()) {
                String targetUrl = "http://" + curServer.getTargetAddress() + SYNC_CLUSTER_INFO;
                MqRequest request = new MqRequest(targetUrl, clusterInfo);
                HttpUtil.postRequest(request);
            }

            AdminServerCache.isSyncClusterInfo = false;
        }
    }


    private static void geClusterInfo(ClusterInfo clusterInfo) {
        // 遍历所有的当前 server，获取得到 queue info map
        clusterInfo.getQueueAmountMap().clear();
        clusterInfo.getQueueOffsetMap().clear();
        clusterInfo.getQueueSizeMap().clear();
        clusterInfo.getKidQueueInfo().clear();
        clusterInfo.getQueuePacketMap().clear();
        clusterInfo.getKidStatusMap().clear();

        for (ServerInfo curServer : clusterInfo.getCurServers()) {
            // 获取 queue info Map
            String targetUrl = "http://" + curServer.getTargetAddress() + GET_BASE_QUEUE_INFO;
            MqRequest request = new MqRequest(targetUrl, null);
            ConcurrentHashMap<String, JSONObject> queueInfoStrMap = HttpUtil.getQueueInfo(request);

            // 设置 kid 的工作状态
            String checkKidUrl = "http://" + curServer.getTargetAddress() + CHECK_KID_STATUS;
            MqRequest checkKidStatusRequest = new MqRequest(checkKidUrl, null);
            String kidStatusResp = HttpUtil.postRequest(checkKidStatusRequest);
            if (!StringUtils.isBlank(kidStatusResp)) {
                clusterInfo.getKidStatusMap().put(
                        curServer.getKid(), kidStatusResp.equals("1") ? 1 : -1);
            }

            // 表示无法链接，那么就移除改 server
            if (null == queueInfoStrMap) {
                AdminServerCache.clusterInfo.get().getCurServers().remove(curServer);
                continue;
            }

            if (null != queueInfoStrMap && queueInfoStrMap.size() > 0) {
                ConcurrentHashMap<String, QueueInfo> queueInfoMap = new ConcurrentHashMap<>();
                Iterator<Map.Entry<String, JSONObject>> iterator = queueInfoStrMap.entrySet().iterator();
                // 遍历得到的 queue info map
                while (iterator.hasNext()) {
                    Map.Entry<String, JSONObject> mapEntry = iterator.next();
                    // 得到 queue info
                    JSONObject jsonObject = mapEntry.getValue();
                    QueueInfo tempQueueInfo = getQueueInfo(jsonObject);

                    String queueName = tempQueueInfo.getName();
                    // 统计 queue 在集群中的数量
                    setQueueAmount(clusterInfo, queueName);
                    // 设置每个 queue 的 offset
                    setQueueOffsetAndSize(clusterInfo, curServer, tempQueueInfo, queueName);
                    // 设置 queue info map
                    queueInfoMap.put(queueName, tempQueueInfo);
                    // 设置 queue server 信息
                    Set<ServerInfo> serverSet = clusterInfo.getQueuePacketMap().getOrDefault(queueName, new HashSet<>());
                    serverSet.add(curServer);
                    clusterInfo.getQueuePacketMap().put(queueName, serverSet);
                }
                clusterInfo.getKidQueueInfo().put(curServer.getKid(), queueInfoMap);
            } else {
                clusterInfo.getKidQueueInfo().put(curServer.getKid(), new ConcurrentHashMap<>());
            }
        }
    }


    private static void setQueueOffsetAndSize(ClusterInfo clusterInfo, ServerInfo curServer, QueueInfo tempQueueInfo, String queueName) {
        Long tempQueueOffset;
        if (tempQueueInfo.getOffset() == null) {
            tempQueueOffset = -1L;
        } else {
            tempQueueOffset = Long.valueOf(tempQueueInfo.getOffset());
        }
        Long curMaxOffset = clusterInfo.getQueueOffsetMap().getOrDefault(queueName, -1L);
        Integer temQueueSize = null == tempQueueInfo.getAmount() ? -1 : tempQueueInfo.getAmount();
        Integer curQueueSize = clusterInfo.getQueueSizeMap().getOrDefault(queueName, 0);
        if (tempQueueOffset.compareTo(curMaxOffset) > 0) {
            clusterInfo.getQueueOffsetMap().put(queueName, tempQueueOffset);
            clusterInfo.getQueueSizeMap().put(queueName, Math.max(temQueueSize, curQueueSize));
        }
        if (temQueueSize > curQueueSize) {
            clusterInfo.getQueueLatestKid().put(queueName, curServer.getKid());
        }

    }

    private static void setQueueAmount(ClusterInfo clusterInfo, String queueName) {
        int amount = clusterInfo.getQueueAmountMap().getOrDefault(queueName, 0);
        clusterInfo.getQueueAmountMap().put(queueName, amount + 1);
    }

    public static QueueInfo getQueueInfo(JSONObject jsonObject) {
        QueueType queueType = null;
        switch (jsonObject.getString("type")) {
            case "PERMANENT":
                queueType = QueueType.PERMANENT;
                break;
            case "DELAY":
                queueType = QueueType.DELAY;
                break;
            default:
                queueType = QueueType.TEMPORARY;
                break;
        }

        return new QueueInfo(
                jsonObject.getString("kid"), jsonObject.getString("name"),
                queueType, jsonObject.getString("offset"),
                jsonObject.getInteger("amount")
        );
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

        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)
                && AdminServerCache.isSyncClusterInfo) {
            log.info("still in sync data with other broker....");
            return false;
        }
        return true;
    }

    public static Map<String, QueueInfo> getLocalQueueInfoMap() {
        return BrokerLocalCache.queueInfoMap;
    }
}

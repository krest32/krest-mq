package com.krest.mq.admin.controller;


import com.alibaba.fastjson.JSONObject;
import com.krest.mq.admin.balancer.BrokerBalancer;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@RestController
@RequestMapping("mq/server")
public class ClusterInfoController {

    String clearHisDataPath = "/mq/manager/clear/overdue/data";
    String checkBrokerQueueInfoPath = "/queue/manager/get/base/queue/info";


    @Autowired
    ClusterUtil clusterUtil;

    /**
     * 获取配置内容
     */
    @GetMapping("config")
    public String getConfig() {
        return SyncDataUtils.mqConfig.toString();
    }

    /**
     * 获取 cluster 中的 server 角色
     */
    @GetMapping("cluster/role")
    public String getClusterRole() {
        return AdminServerCache.clusterRole.toString();
    }

    /**
     * 获取 Cluster 的信息内容
     */
    @GetMapping("cluster/info")
    public ClusterInfo getClusterInfo() {
        return AdminServerCache.clusterInfo;
    }


    /**
     * 同步 cluster info 信息
     */
    @PostMapping("sync/cluster-info")
    public void syncClusterInfo(@RequestBody ClusterInfo clusterInfo) {
        AdminServerCache.clusterInfo = clusterInfo;
    }


    /**
     * 向 leader 中注册 follower，
     * 然后清空自身之前存在的数据信息，以便更好的接入集群中
     * todo 逻辑存在漏洞，待修改
     */
    @PostMapping("register")
    public ServerInfo register(@RequestBody ServerInfo serverInfo) throws InterruptedException {
        // 如果正在选择 Leader, 那么就进入等到状态
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            log.info("receive new service register : " + serverInfo);
            // 添加当前 leader的服务中
            AdminServerCache.curServers.add(serverInfo);
            // 同步集群的 queue 信息
            SyncDataUtils.syncClusterInfo();
            // 进行检查， 判断新注册的节点上的队列是否在集群中存在，只要存在一条就全部删除
            if (checkBrokerQueueInfo(serverInfo)) {
                // 清空原始数据
                clearHisData(serverInfo);
                // 同步集群的 queue 信息
                SyncDataUtils.syncClusterInfo();
            }
            // 进行负载均很
            BrokerBalancer.run();
            // 同步集群的 queue 信息
            SyncDataUtils.syncClusterInfo();
            // 返回 Leader 的信息
            return AdminServerCache.leaderInfo;
        }
        // 返回一个空对象
        return null;
    }

    /**
     * 检查注册进来的 broker 是否要清除数据
     * 1. 新注册的 broker 的 queue 当前集群没有的 false
     * 2. 新注册的 broke 的 queue 是当前集群存在的
     */
    private boolean checkBrokerQueueInfo(ServerInfo serverInfo) {
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
                Long tempOffset = Long.valueOf(tempQueueInfo.getOffset() == null
                        ? "-1L" : tempQueueInfo.getOffset());
                Integer tempSize = tempQueueInfo.getAmount() == null
                        ? -1 : tempQueueInfo.getAmount();

                // 检查 offset 和 amount, 如果 offset 和 amount 有一处不一致，那么就删除
                Long offset = Long.valueOf(AdminServerCache.clusterInfo.getQueueOffsetMap().get(queueName) == null
                        ? -1 : AdminServerCache.clusterInfo.getQueueOffsetMap().get(queueName));
                Integer size = AdminServerCache.clusterInfo.getQueueSizeMap().get(queueName) == null
                        ? -1 : AdminServerCache.clusterInfo.getQueueSizeMap().get(queueName);

                // 如果 集群的偏移量 大于 新注册的 broker, 说明 broker 的数据是旧的， 执行删除
                if (offset.compareTo(tempOffset) > 0) {
                    System.out.println(1);
                    return true;
                }
                // 如果 偏移量相同，但是新注册的 broker queue size 大于 cluster 中的 queue size，
                // 那就说明 新的 broker 数据更新
                if (offset.compareTo(tempOffset) == 0 && size > tempSize) {
                    System.out.println(3);
                    return true;
                }

                // 判断 queue 在 cluster 中数量是否超过了 最大副本数
                Integer amount = AdminServerCache.clusterInfo.getQueueAmountMap()
                        .getOrDefault(queueName, 0);
                if (amount > AdminServerCache.clusterInfo.getDuplicate()) {
                    System.out.println(3);
                    return true;
                }
            }
        }
        return false;
    }

    private void clearHisData(ServerInfo serverInfo) {
        String targetUrl = "http:\\" + serverInfo.getTargetAddress() + clearHisDataPath;
        MqRequest request = new MqRequest(targetUrl, null);
        HttpUtil.postRequest(request);
    }

    /**
     * follower 反向检测 leader
     */
    @PostMapping("check/leader")
    public String checkLeader(@RequestBody ServerInfo serverInfo) {
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            if (AdminServerCache.curServers.add(serverInfo)) {
                log.info("receive be lost register : " + serverInfo);
            }
        }
        AdminServerCache.resetExpireTime();
        return "ok";
    }

    /**
     * leader 检测 follower
     */
    @PostMapping("check/follower")
    public String checkFollower(@RequestBody ServerInfo serverInfo) {
        // 如果发来的 Leader 信息，不同于自己的认证 Leader, 那么就需要进入重新选举状态
        if (null != AdminServerCache.leaderInfo) {
            if (!serverInfo.getTargetAddress().equals(AdminServerCache.leaderInfo.getTargetAddress())) {
                log.info("cluster leader have more than one");
                String reSelectPath = "/mq/server/reselect/leader";
                // 通知所有的 server 进入到重新选举的状态
                for (ServerInfo server : SyncDataUtils.mqConfig.getServerList()) {
                    String targetUrl = "http://" + server.getTargetAddress() + reSelectPath;
                    MqRequest request = new MqRequest(targetUrl, null);
                    HttpUtil.getRequest(request);
                }
            }
        }
        // 重置过期时间
        AdminServerCache.resetExpireTime();
        return "ok";
    }


    /**
     * 开始重新选举 leader
     */
    @GetMapping("reselect/leader")
    public void reSelectLeader() {
        log.info("start re select cluster leader...");
        clusterUtil.initData();
        LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(SyncDataUtils.mqConfig));
    }

    /**
     * 普通选举 leader
     */
    @PostMapping("select/leader")
    public String selectLeader(@RequestBody ServerInfo server) {
        // 要选举的 Leader 信息
        String targetAddress = "http://" + server.getTargetAddress() + "/mq/server/check/connect";
        boolean flag = HttpUtil.getRequest(new MqRequest(targetAddress, null));
        // 同意是 0， 不同意为 1
        return flag ? "0" : "1";
    }

}


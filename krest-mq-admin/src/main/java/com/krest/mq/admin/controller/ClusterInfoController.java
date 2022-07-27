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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;


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

    @GetMapping("cur/servers")
    public CopyOnWriteArraySet<ServerInfo> getCurServices() {
        if (AdminServerCache.clusterInfo.equals(ClusterRole.Leader)) {
            return AdminServerCache.curServers;
        }
        return null;
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
        SyncDataUtils.syncClusterInfo();
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
     * 向 leader 中注册 follower
     */
    @PostMapping("register")
    public ServerInfo register(@RequestBody ServerInfo serverInfo)  {
        // 如果正在选择 Leader, 那么就进入等到状态
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            log.info("receive new service register : " + serverInfo);
            // 添加当前 leader的服务中
            AdminServerCache.curServers.add(serverInfo);
            return AdminServerCache.leaderInfo;
        }
        // 返回一个空对象
        return null;
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


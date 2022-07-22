package com.krest.mq.admin.controller;


import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.ClusterInfo;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@Slf4j
@RestController
@RequestMapping("mq/server")
public class ClusterController {

    @Autowired
    MqConfig mqConfig;

    @Autowired
    ClusterUtil clusterUtil;

    @GetMapping("config")
    public String getConfig() {
        return mqConfig.toString();
    }

    @GetMapping("cluster/role")
    public String getClusterRole() {
        return AdminServerCache.clusterRole.toString();
    }

    @GetMapping("cluster/info")
    public ClusterInfo getClusterInfo() {
        return AdminServerCache.clusterInfo;
    }


    /**
     * todo 逻辑存在漏洞，待修改
     * @param serverInfo
     * @return
     * @throws InterruptedException
     */
    @PostMapping("register")
    public ServerInfo register(@RequestBody ServerInfo serverInfo) throws InterruptedException {
        // 如果正在选择 Leader, 那么就进入等到状态
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            log.info("receive new service register : " + serverInfo);
            AdminServerCache.curServers.add(serverInfo);

            // 同步集群的 queue 信息
            SyncDataUtils.collectQueueInfo();
            return AdminServerCache.leaderInfo;
        }
        // 返回一个空对象
        return null;
    }

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

    @PostMapping("check/follower")
    public String checkFollower(@RequestBody ServerInfo serverInfo) {
        // 如果发来的 Leader 信息，不同于自己的认证 Leader, 那么就需要进入重新选举状态
        if (null != AdminServerCache.leaderInfo) {
            if (!serverInfo.getTargetAddress().equals(AdminServerCache.leaderInfo.getTargetAddress())) {
                log.info("cluster leader have more than one");
                String reSelectPath = "/mq/server/reselect/leader";
                // 通知所有的 server 进入到重新选举的状态
                for (ServerInfo server : this.mqConfig.getServerList()) {
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


    @GetMapping("reselect/leader")
    public void reSelectLeader() {
        log.info("start re select cluster leader...");
        clusterUtil.initData();
        LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(this.mqConfig));
    }

    @PostMapping("select/leader")
    public String selectLeader(@RequestBody ServerInfo server) {
        // 要选举的 Leader 信息
        String targetAddress = "http://" + server.getTargetAddress() + "/mq/server/check/connect";
        boolean flag = HttpUtil.getRequest(new MqRequest(targetAddress, null));
        // 同意是 0， 不同意为 1
        return flag ? "0" : "1";
    }

    /**
     * 同步 cluster info 信息
     */
    @PostMapping("synch/cluster-info")
    public void synchClusterInfo(@RequestBody ClusterInfo clusterInfo) {
        AdminServerCache.clusterInfo = clusterInfo;
    }
}


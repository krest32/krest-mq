package com.krest.mq.admin.controller;

import com.krest.mq.admin.cache.AdminCache;
import com.krest.mq.admin.entity.MqRequest;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.HttpUtil;
import com.krest.mq.core.entity.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.exeutor.LocalExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@Slf4j
@RestController
@RequestMapping("mq/server")
public class Controller {

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
        return AdminCache.clusterRole.toString();
    }

    @PostMapping("register")
    public String register(@RequestBody ServerInfo serverInfo) throws InterruptedException {
        // 如果正在选择 Leader, 那么就进入等到状态
        if (AdminCache.clusterRole.equals(ClusterRole.Leader)) {
            log.info("receive new service register : " + serverInfo);
            AdminCache.curServers.add(serverInfo);
        }
        return AdminCache.clusterRole.toString();
    }

    @PostMapping("check/leader")
    public String checkLeader(@RequestBody ServerInfo serverInfo) {
        if (AdminCache.clusterRole.equals(ClusterRole.Leader)) {
            if (AdminCache.curServers.add(serverInfo)) {
                log.info("receive be lost register : " + serverInfo);
            }
        }
        AdminCache.resetExpireTime();
        return "ok";
    }

    @PostMapping("check/follower")
    public String checkFollower(@RequestBody ServerInfo serverInfo) {
        // 如果发来的 Leader 信息，不同于自己的认证 Leader, 那么就需要进入重新选举状态
        if (null != AdminCache.leaderInfo && !serverInfo.equals(AdminCache.leaderInfo)) {
            log.info("cluster leader have more than one");
            String reSelectPath = "/mq/server/reselect/leader";
            for (String server : this.mqConfig.getCluster()) {
                String targetUrl = "http://" + server + reSelectPath;
                MqRequest request = new MqRequest(targetUrl, null);
                HttpUtil.getRequest(request);
            }
        }
        // 重置过期时间
        AdminCache.resetExpireTime();
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
}


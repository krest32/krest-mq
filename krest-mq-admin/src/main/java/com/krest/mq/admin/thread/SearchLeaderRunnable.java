package com.krest.mq.admin.thread;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.SynchUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class SearchLeaderRunnable implements Runnable {

    String registerPath = "/mq/server/register";
    String selectLeaderPath = "/mq/server/select/leader";

    MqConfig mqConfig;


    public SearchLeaderRunnable(MqConfig config) {
        this.mqConfig = config;
    }

    @Override
    public void run() {

        AdminServerCache.isSelectServer = true;

        if (null == AdminServerCache.leaderInfo) {
            // 寻找 Leader
            searchLeader();
            // 选举 Leader
            selectLeader();
        }

        AdminServerCache.isSelectServer = false;

    }


    private void searchLeader() {

        // 遍历所有的信息
        for (int i = 0; i < this.mqConfig.getServerList().size(); i++) {
            ServerInfo curServerInfo = this.mqConfig.getServerList().get(i);
            AdminServerCache.kidServerMap.put(curServerInfo.getKid(), curServerInfo);

            // 如果当前的角色不是 观察模式 就退出
            if (!AdminServerCache.clusterRole.equals(ClusterRole.Observer)
                    && null != AdminServerCache.leaderInfo) {
                log.info("Leader 信息：" + AdminServerCache.leaderInfo);
                break;
            }

            // 开始寻找 leader
            String targetUtl = "http://" + curServerInfo.getTargetAddress() + registerPath;
            MqRequest request = new MqRequest(targetUtl, AdminServerCache.selfServerInfo);
            String responseStr = HttpUtil.postRequest(request);

            // 接口返回 null
            if (StringUtils.isBlank(responseStr)) {
                AdminServerCache.curServers.add(curServerInfo);
            }

            if (!"error".equals(responseStr)) {
                ServerInfo tempServer = JSONObject.parseObject(responseStr, ServerInfo.class);
                // 如果找到了 Leader, 那么就设置信息
                AdminServerCache.leaderInfo = tempServer;
                AdminServerCache.clusterRole = ClusterRole.Follower;
            }
        }
    }


    private void selectLeader() {
        // 如果没有站到 leader， 就需要进入到选举 leader 的模式
        if (null == AdminServerCache.leaderInfo
                || AdminServerCache.clusterRole.equals(ClusterRole.Observer)) {

            log.info("start select leader...");
            List<Map.Entry<String, ServerInfo>> serverList = AdminServerCache.getSelectServerList();
            ServerInfo selectedServer = null;

            // 找到最大的 kid server info
            for (Map.Entry<String, ServerInfo> entry : serverList) {
                if (AdminServerCache.curServers.contains(entry.getValue())) {
                    if (null == selectedServer) {
                        selectedServer = entry.getValue();
                        break;
                    }
                }
            }

            // 如果自身就是最大的,就跳过，否则发送选举信息
            Integer agreeCount = 0;
            for (Map.Entry<String, ServerInfo> entry : serverList) {
                // 如果当前的 server 存活者，那就发送选举
                if (AdminServerCache.curServers.contains(entry.getValue())) {
                    MqRequest request = new MqRequest(
                            "http://" + entry.getValue().getTargetAddress() + selectLeaderPath,
                            selectedServer);
                    String ans = HttpUtil.postRequest(request);

                    if (null != ans) {
                        if (ans.equals("0")) {
                            agreeCount++;
                        } else {
                            if (entry.getKey().equals(AdminServerCache.kid)) {
                                continue;
                            }

                            // 如果返回了 1, 证明有的 server 无法连接当前 server
                            log.error(entry.getValue().getTargetAddress()
                                    + " can not connect " +
                                    selectedServer.getTargetAddress());
                        }
                    }
                }
            }

            if (agreeCount >= (AdminServerCache.curServers.size() / 2)) {

                if (selectedServer == null) {
                    selectedServer = AdminServerCache.selfServerInfo;
                }

                AdminServerCache.leaderInfo = selectedServer;

                if (!selectedServer.getTargetAddress()
                        .equals(AdminServerCache.selfServerInfo.getTargetAddress())) {
                    AdminServerCache.clusterRole = ClusterRole.Follower;
                } else {
                    AdminServerCache.clusterRole = ClusterRole.Leader;
                }

                log.info("select leader success, leader info : " + selectedServer);
                log.info("select leader success, cluster role info : " + AdminServerCache.clusterRole);
            }
        }
    }
}

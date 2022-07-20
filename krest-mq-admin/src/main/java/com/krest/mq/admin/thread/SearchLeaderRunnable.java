package com.krest.mq.admin.thread;

import com.krest.mq.admin.cache.AdminCache;
import com.krest.mq.admin.entity.MqRequest;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.util.HttpUtil;
import com.krest.mq.core.entity.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class SearchLeaderRunnable implements Runnable {

    static final String registerPath = "/mq/server/register";
    static final String selectLeaderPath = "/mq/server/select/leader";
    MqConfig mqConfig;


    public SearchLeaderRunnable(MqConfig config) {
        this.mqConfig = config;
    }

    @Override
    public void run() {

        AdminCache.isSelectServer = true;

        if (null == AdminCache.leaderInfo) {
            // 寻找 Leader
            searchLeader();
            // 选举 Leader
            selectLeader();
        }

        AdminCache.isSelectServer = false;
    }


    private void searchLeader() {
        if (null != this.mqConfig.getCluster() && this.mqConfig.getCluster().size() > 0) {
            if (this.mqConfig.getCluster().size() != this.mqConfig.getKids().size()) {
                log.error("please check config for cluster and kids");
            } else {
                for (int i = 0; i < this.mqConfig.getCluster().size(); i++) {
                    String kid = this.mqConfig.getKids().get(i);
                    String[] info = this.mqConfig.getCluster().get(i).split(":");
                    ServerInfo curServerInfo = new ServerInfo(info[0], Integer.valueOf(info[1]));
                    if (kid.equals(AdminCache.kid)) {
                        AdminCache.selfServerInfo = curServerInfo;
                        break;
                    }
                }

                for (int i = 0; i < this.mqConfig.getCluster().size(); i++) {
                    String kid = this.mqConfig.getKids().get(i);
                    String[] info = this.mqConfig.getCluster().get(i).split(":");
                    ServerInfo curServerInfo = new ServerInfo(info[0], Integer.valueOf(info[1]));
                    AdminCache.kidServerMap.put(kid, curServerInfo);

                    // 如果当前的角色不是 观察模式 就退出
                    if (!AdminCache.clusterRole.equals(ClusterRole.Observer)) {
                        log.info("Leader 信息：" + AdminCache.leaderInfo);
                        break;
                    }
                    // 开始寻找 leader
                    String targetUtl = "http://" + this.mqConfig.getCluster().get(i) + registerPath;
                    MqRequest request = new MqRequest(targetUtl, AdminCache.selfServerInfo);
                    String responseStr = HttpUtil.postRequest(request);
                    if (null != responseStr) {
                        AdminCache.curServers.add(curServerInfo);
                        // 如果找到了 Leader, 那么就设置信息
                        if (responseStr.equals(ClusterRole.Leader.toString())) {
                            AdminCache.leaderInfo = curServerInfo;
                            AdminCache.clusterRole = ClusterRole.Follower;
                        }
                    }
                }
            }
        }
    }


    private void selectLeader() {

        // 如果没有站到 leader， 就需要进入到选举 leader 的模式
        if (null == AdminCache.leaderInfo
                || AdminCache.clusterRole.equals(ClusterRole.Observer)) {

            log.info("start select leader...");
            List<Map.Entry<String, ServerInfo>> serverList = AdminCache.getSelectServerList();
            ServerInfo selectedServer = null;

            // 找到最大的 kid server info
            for (Map.Entry<String, ServerInfo> entry : serverList) {
                if (AdminCache.curServers.contains(entry.getValue())) {
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
                if (AdminCache.curServers.contains(entry.getValue())) {
                    MqRequest request = new MqRequest(
                            "http://" + entry.getValue().getTargetAddress() + selectLeaderPath,
                            selectedServer);
                    String ans = HttpUtil.postRequest(request);

                    if (null != ans) {
                        if (ans.equals("0")) {
                            agreeCount++;
                        } else {
                            if (entry.getKey().equals(AdminCache.kid)) {
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
            if (agreeCount >= (AdminCache.curServers.size() / 2)) {
                AdminCache.leaderInfo = selectedServer;
                if (!selectedServer.equals(AdminCache.selfServerInfo)) {
                    AdminCache.clusterRole = ClusterRole.Follower;
                } else {
                    AdminCache.clusterRole = ClusterRole.Leader;
                }
                log.info("select leader success, leader info : " + selectedServer);
                log.info("select leader success, cluster role info : " + AdminCache.clusterRole);
            }
        }
    }
}

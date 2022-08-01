package com.krest.mq.admin.thread;


import com.alibaba.fastjson.JSONObject;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        if (AdminServerCache.isSelectServer)
            return;

        AdminServerCache.isSelectServer = true;
        searchLeader();
        selectLeader();
        AdminServerCache.isSelectServer = false;
    }

    /**
     * 寻找 Leader
     */
    private void searchLeader() {

        // 如果自己就是Leader 那么就不需要寻找
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            return;
        }


        // 如果此时的 leader 信息不为 null，那就进行探测
        if (null != AdminServerCache.leaderInfo) {
            boolean flag = ClusterUtil.detectLeader(
                    AdminServerCache.leaderInfo.getTargetAddress(),
                    AdminServerCache.selfServerInfo
            );
            // 探测失败
            if (!flag) {
                AdminServerCache.leaderInfo = null;
                AdminServerCache.clusterRole = ClusterRole.Observer;
            } else {
                AdminServerCache.clusterRole = ClusterRole.Follower;
                log.info("leader info : {} ", AdminServerCache.leaderInfo);
                return;
            }
        }

        AdminServerCache.clusterInfo.get().getCurServers().clear();

        // 遍历所有的信息
        for (int i = 0; i < this.mqConfig.getServerList().size(); i++) {
            ServerInfo curServerInfo = this.mqConfig.getServerList().get(i);

            // 跳过自己
            if (curServerInfo.getKid().equals(AdminServerCache.kid)) {
                AdminServerCache.clusterInfo.get().getCurServers().add(curServerInfo);
                continue;
            }

            // 开始寻找 leader
            String targetUtl = "http://" + curServerInfo.getTargetAddress() + registerPath;
            MqRequest request = new MqRequest(targetUtl, AdminServerCache.selfServerInfo);
            String responseStr = HttpUtil.postRequest(request);

            // 接口返回 null, 说明集群正在选举
            if (StringUtils.isBlank(responseStr)) {
                AdminServerCache.clusterInfo.get().getCurServers().add(curServerInfo);
                continue;
            }
            // 接口返回 leader 信息
            if (!"error".equals(responseStr)) {
                ServerInfo tempServer = JSONObject.parseObject(responseStr, ServerInfo.class);
                AdminServerCache.clusterInfo.get().getCurServers().add(curServerInfo);

                // 检查返回的信息
                boolean flag = ClusterUtil.detectLeader(
                        tempServer.getTargetAddress(),
                        AdminServerCache.selfServerInfo
                );
                // 探测失败
                if (!flag) {
                    AdminServerCache.leaderInfo = null;
                    AdminServerCache.clusterRole = ClusterRole.Observer;
                } else {
                    log.info("get leader info from : {}, leader is : {}  ", curServerInfo.getTargetAddress(), tempServer.getTargetAddress());
                    boolean isLeaderOK = ClusterUtil.detectLeader(tempServer.getTargetAddress(), AdminServerCache.selfServerInfo);
                    if (isLeaderOK) {
                        AdminServerCache.leaderInfo = tempServer;
                        AdminServerCache.clusterRole = ClusterRole.Follower;
                        return;
                    } else {
                        log.info("can not connect to : {} ", tempServer.getTargetAddress());
                    }
                }
            }
        }
    }

    /**
     * 如果没有找到leader， 那么就需要重新选举 leader
     */
    private synchronized void selectLeader() {
        // 如果没有站到 leader， 就需要进入到选举 leader 的模式
        if (null == AdminServerCache.leaderInfo
                || AdminServerCache.clusterRole.equals(ClusterRole.Observer)) {

            log.info("start select leader...");
            // 转化为 list, 同时根据 kid 降序排序
            List<ServerInfo> curServers = new ArrayList<>(AdminServerCache.clusterInfo.get().getCurServers());
            curServers.sort((o1, o2) ->
                    Integer.valueOf(o2.getKid()).compareTo(Integer.valueOf(o1.getKid()))
            );

            AdminServerCache.selectedServer = curServers.get(curServers.size() - 1);

            for (ServerInfo serverInfo : curServers) {
                // 如果当前的 server 存活者，那就发送选举
                MqRequest request = new MqRequest(
                        "http://" + serverInfo.getTargetAddress() + selectLeaderPath,
                        AdminServerCache.selectedServer);
                String ans = HttpUtil.postRequest(request);
                if (null != ans) {
                    // 如果同意，就继续通知其他 server， 否则开始重新查找
                    if (ans.equals("1")) {
                        continue;
                    } else {
                        run();
                    }
                }
            }
            if (AdminServerCache.selectedServer.getKid().equals(AdminServerCache.kid)) {
                AdminServerCache.clusterRole = ClusterRole.Leader;
            } else {
                AdminServerCache.clusterRole = ClusterRole.Follower;
            }
            AdminServerCache.leaderInfo = AdminServerCache.selectedServer;
            log.info("select leader success, leader info : " + AdminServerCache.leaderInfo.getTargetAddress());
            log.info("select leader success, cluster role info : " + AdminServerCache.clusterRole);


            // 选举结束通知其他 server
            if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
                Iterator<Map.Entry<String, ServerInfo>> iterator = AdminServerCache.getSelectServerList().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, ServerInfo> next = iterator.next();
                    ServerInfo serverInfo = next.getValue();
                    ClusterUtil.detectFollower(serverInfo.getTargetAddress(), AdminServerCache.leaderInfo);
                }
            }
        }
    }
}

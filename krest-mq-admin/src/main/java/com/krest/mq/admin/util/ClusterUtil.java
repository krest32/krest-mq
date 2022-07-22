package com.krest.mq.admin.util;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class ClusterUtil {

    String detectLeaderPath = "/mq/server/check/leader";
    String detectFollowerPath = "/mq/server/check/follower";
    String registerPath = "/mq/server/register";


    public boolean detectLeader(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                "http://" + leaderAddress + detectLeaderPath, serverInfo);
        String response = HttpUtil.postRequest(request);
        return !"error".equals(response);
    }

    public boolean detectFollower(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                "http://" + leaderAddress + detectFollowerPath, serverInfo);
        String response = HttpUtil.postRequest(request);
        return !"error".equals(response);
    }

    public void registerSelf() {
        String targetUtl = "http://" + AdminServerCache.leaderInfo.getTargetAddress() + registerPath;
        MqRequest request = new MqRequest(targetUtl, AdminServerCache.selfServerInfo);
        HttpUtil.postRequest(request);
    }


    public void initData() {
        AdminServerCache.leaderInfo = null;
        AdminServerCache.clusterRole = ClusterRole.Observer;
        AdminServerCache.curServers.clear();
        AdminServerCache.kidServerMap.clear();
    }

    public boolean isReady() {
        if (AdminServerCache.isSelectServer) {
            log.info("still in select server....");
            return false;
        }
        if (AdminServerCache.isDetectFollower) {
            log.info("still in detect follower....");
            return false;
        }
        return true;
    }
}

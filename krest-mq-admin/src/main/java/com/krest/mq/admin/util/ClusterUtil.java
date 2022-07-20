package com.krest.mq.admin.util;

import com.krest.mq.admin.cache.AdminCache;
import com.krest.mq.admin.entity.MqRequest;
import com.krest.mq.core.entity.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ClusterUtil {

    String detectLeaderPath = "/mq/server/check/leader";
    String detectFollowerPath = "/mq/server/check/follower";

    public boolean detectLeader(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                "http://" + leaderAddress + detectLeaderPath, serverInfo);
        String response = HttpUtil.postRequest(request);
        return !StringUtils.isBlank(response);
    }

    public boolean detectFollower(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                "http://" + leaderAddress + detectFollowerPath, serverInfo);
        String response = HttpUtil.postRequest(request);
        return !StringUtils.isBlank(response);
    }


    public void initData() {
        AdminCache.leaderInfo = null;
        AdminCache.clusterRole = ClusterRole.Observer;
        AdminCache.selfServerInfo = null;
        AdminCache.curServers.clear();
        AdminCache.kidServerMap.clear();
    }

    public boolean isReady() {
        if (AdminCache.isSelectServer) {
            log.info("still in select server....");
            return false;
        }
        if (AdminCache.isDetectFollower) {
            log.info("still in detect follower....");
            return false;
        }

        return true;
    }
}

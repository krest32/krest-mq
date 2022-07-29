package com.krest.mq.admin.util;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Slf4j
public class ClusterUtil {

    static String detectLeaderPath = "/mq/server/check/leader";
    static String detectFollowerPath = "/mq/server/check/follower";
    static String registerPath = "/mq/server/register";


    public static boolean detectLeader(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                "http://" + leaderAddress + detectLeaderPath, serverInfo);
        String response = HttpUtil.postRequest(request);
        if (StringUtils.isBlank(response))
            return false;
        return !"error".equals(response);
    }

    public static boolean detectFollower(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                "http://" + leaderAddress + detectFollowerPath, serverInfo);
        String response = HttpUtil.postRequest(request);
        return !"error".equals(response);
    }

    public static String registerSelf() {
        String targetUtl = "http://" + AdminServerCache.leaderInfo.getTargetAddress() + registerPath;
        MqRequest request = new MqRequest(targetUtl, AdminServerCache.selfServerInfo);
        return HttpUtil.postRequest(request);
    }


    /**
     * 清空当前的服务缓存的数据信息
     */
    public static void initData() {
        AdminServerCache.leaderInfo = null;
        AdminServerCache.clusterInfo.getQueueLatestKid().clear();
        AdminServerCache.clusterInfo.getKidQueueInfo().clear();
        AdminServerCache.clusterInfo.getQueueAmountMap().clear();
        AdminServerCache.clusterInfo.getQueueSizeMap().clear();
        AdminServerCache.clusterInfo.getQueueOffsetMap().clear();

        AdminServerCache.clusterRole = ClusterRole.Observer;
        AdminServerCache.clusterInfo.getCurServers().clear();
    }

}

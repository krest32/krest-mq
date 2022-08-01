package com.krest.mq.admin.util;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ClusterUtil {

    private ClusterUtil() {
    }

    static final String DETECT_LEADER_PATH = "/mq/server/check/leader";
    static final String DETECT_FOLLOWER_PATH = "/mq/server/check/follower";
    static final String REGISTER_PATH = "/mq/server/register";
    static final String HTTP_START_STR = "http://";

    public static boolean detectLeader(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                HTTP_START_STR + leaderAddress + DETECT_LEADER_PATH, serverInfo);
        String response = HttpUtil.postRequest(request);
        if (StringUtils.isBlank(response)) {
            return false;
        }
        return !"error".equals(response);
    }

    public static boolean detectFollower(String leaderAddress, ServerInfo serverInfo) {
        MqRequest request = new MqRequest(
                HTTP_START_STR + leaderAddress + DETECT_FOLLOWER_PATH, serverInfo);
        String response = HttpUtil.postRequest(request);
        return !"error".equals(response);
    }

    public static String registerSelf() {
        String targetUtl = HTTP_START_STR + AdminServerCache.leaderInfo.getTargetAddress() + REGISTER_PATH;
        MqRequest request = new MqRequest(targetUtl, AdminServerCache.selfServerInfo);
        return HttpUtil.postRequest(request);
    }


    /**
     * 清空当前的服务缓存的数据信息
     */
    public static void initData() {
        AdminServerCache.leaderInfo = null;
        AdminServerCache.clusterInfo.get().getQueueLatestKid().clear();
        AdminServerCache.clusterInfo.get().getKidQueueInfo().clear();
        AdminServerCache.clusterInfo.get().getQueueAmountMap().clear();
        AdminServerCache.clusterInfo.get().getQueueSizeMap().clear();
        AdminServerCache.clusterInfo.get().getQueueOffsetMap().clear();
        AdminServerCache.clusterRole = ClusterRole.Observer;
    }
}

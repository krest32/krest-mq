package com.krest.mq.admin.cache;


import com.krest.mq.core.entity.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class AdminCache {

    public static Long expireTime;
    public static ServerInfo leaderInfo;
    public static ServerInfo selfServerInfo;
    public static boolean isSelectServer = false;
    public static boolean isDetectFollower = false;
    public static ClusterRole clusterRole = ClusterRole.Observer;
    public static CopyOnWriteArraySet<ServerInfo> curServers = new CopyOnWriteArraySet<>();
    public static ConcurrentHashMap<String, ServerInfo> kidServerMap = new ConcurrentHashMap<>();
    public static String kid;

    public static List<Map.Entry<String, ServerInfo>> getSelectServerList() {
        List<Map.Entry<String, ServerInfo>> serverList = new ArrayList<>();
        Iterator<Map.Entry<String, ServerInfo>> iterator = AdminCache.kidServerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            serverList.add(iterator.next());
        }
        // 通过 kid 进行降序排序
        serverList.sort((o1, o2) -> Integer.valueOf(o2.getKey()).compareTo(Integer.valueOf(o1.getKey())));
        return serverList;
    }

    public static void resetExpireTime() {
        expireTime = System.currentTimeMillis() + 15 * 1000;
    }
}

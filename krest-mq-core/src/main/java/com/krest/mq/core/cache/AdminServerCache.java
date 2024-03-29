package com.krest.mq.core.cache;

import com.krest.mq.core.entity.*;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.handler.UdpRespFutureHandler;
import com.krest.mq.core.server.MQUDPServer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class AdminServerCache {

    public static UdpRespFutureHandler udpRespFutureHandler = new UdpRespFutureHandler(4);
    // 异步结结 Future 集合
    public static ConcurrentMap<String, MQRespFuture> respFutureMap = new ConcurrentHashMap<>();
    // ack 模式结果处理集合
    public static BlockingDeque<MQMessage.MQEntity> responseQueue = new LinkedBlockingDeque<>();

    // follower 过期时间
    public static Long expireTime;
    // mq server 全局唯一 id
    public static String kid;
    // leader 信息
    public static ServerInfo leaderInfo;
    // 自己的 server 信息
    public static ServerInfo selfServerInfo;
    // 当前 server 是否正在选举 leader
    public static boolean isSelectServer = false;
    // 当前 server 是否正在探测 follower
    public static boolean isDetectFollower = false;
    // 当前 server 是否正在与 其他节点同步数据
    public static boolean isSyncClusterInfo = false;
    // 当前 server 是否正在负载均衡
    public static boolean isKidBalanced = false;
    // 当前节点正在传输数据
    public static boolean isSyncData = false;

    // 默认 server 的状态为 observer （观察者）
    public static ClusterRole clusterRole = ClusterRole.OBSERVER;


    // 配置信息中的 kid 与 cluster server 的对应关系
    public static ConcurrentHashMap<String, ServerInfo> kidServerMap = new ConcurrentHashMap<>();

    // 同步的目标 queue info map
    public static Map<String, QueueInfo> syncTargetQueueInfoMap = new HashMap<>();

    // 普通同步队列
    public static Map<String, BlockingDeque<MQMessage.MQEntity>> syncNormalQueueMap = new ConcurrentHashMap<>();
    // 延迟同步队列
    public static Map<String, DelayQueue<DelayMessage>> syncDelayQueueMap = new ConcurrentHashMap<>();


    /**
     * 记录的 Cluster 信息
     */
    public final static AtomicReference<ClusterInfo> clusterInfo = new AtomicReference<>(new ClusterInfo());
    public static MQUDPServer mqudpServer;
    public static ServerInfo selectedServer;


    // 重置反向探测的过期时间
    public static List<Map.Entry<String, ServerInfo>> getSelectServerList() {
        List<Map.Entry<String, ServerInfo>> serverList = new ArrayList<>();
        Iterator<Map.Entry<String, ServerInfo>> iterator = AdminServerCache.kidServerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            serverList.add(iterator.next());
        }
        // 通过 kid 进行降序排序
        serverList.sort((o1, o2) -> Integer.valueOf(o2.getKey()).compareTo(Integer.valueOf(o1.getKey())));
        return serverList;
    }

    public static void resetExpireTime() {
        expireTime = System.currentTimeMillis() + 45 * 1000;
    }
}

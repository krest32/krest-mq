package com.krest.mq.admin.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.util.FileWriterUtils;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.schedule.DetectFollowerJob;
import com.krest.mq.admin.schedule.DetectLeaderJob;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.HttpUtil;
import com.krest.mq.core.utils.SyncUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@RestController
@RequestMapping("mq/manager")
public class ClusterManagerController {


    @Autowired
    MqConfig mqConfig;

    @Autowired
    DetectFollowerJob detectFollowerJob;

    @Autowired
    DetectLeaderJob detectLeaderJob;


    /**
     * 获取当前 server 的 leader 信息
     */
    @PostMapping("get/leader/info")
    public ServerInfo getLeaderInfo() {
        // 判断当前的 server 是否在非正常状态
        if (!AdminServerCache.clusterRole.equals(ClusterRole.Observer)) {
            // 反向检测 leader 是否存活，
            if (AdminServerCache.leaderInfo != null) {
                boolean flag = ClusterUtil.detectLeader(
                        AdminServerCache.leaderInfo.getTargetAddress(), AdminServerCache.leaderInfo);
                if (!flag) {
                    synchronized (this) {
                        if (!AdminServerCache.isSelectServer) {
                            log.info("无法链接 leader, 开始重新选举 Leader");
                            ClusterUtil.initData();
                            LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(SyncDataUtils.mqConfig));
                        }
                    }
                } else {
                    return AdminServerCache.leaderInfo;
                }
            }

        }
        return null;
    }

    @PostMapping("change/kid/status")
    public String changeKidStatus(@RequestBody String queueInfoMapStr) {
        if (AdminServerCache.isSyncData && "-1".equals(isReadySyncData())) {
            return "-1";
        }
        Map<String, JSONObject> mapStr = JSONObject.parseObject(queueInfoMapStr, HashMap.class);
        Map<String, QueueInfo> targetQueueInfoMap = new HashMap<>();
        Iterator<Map.Entry<String, JSONObject>> iterator = mapStr.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, JSONObject> next = iterator.next();
            String queueName = next.getKey();
            JSONObject tempJsonObject = next.getValue();
            QueueInfo queueInfo = SyncDataUtils.getQueueInfo(tempJsonObject);
            targetQueueInfoMap.put(queueName, queueInfo);
        }
        AdminServerCache.syncTargetQueueInfoMap = targetQueueInfoMap;
        return "1";
    }


    /**
     * 检查当前 Server 的状态
     */
    @PostMapping("check/status")
    public String isReadySyncData() {
        if (AdminServerCache.isSyncData) {
            if (null != AdminServerCache.syncTargetQueueInfoMap
                    && AdminServerCache.syncTargetQueueInfoMap
                    .equals(SyncDataUtils.getLocalQueueInfoMap())) {
                AdminServerCache.isSyncData = false;
                AdminServerCache.syncTargetQueueInfoMap = null;
                return "1";
            }
            return "-1";
        }
        return "1";
    }


    @PostMapping("sync/cluster/info")
    public void syncClusterInfo(@RequestBody String requestStr) {
        // 同步 cluster 信息
        AdminServerCache.clusterInfo = JSONObject.parseObject(requestStr, ClusterInfo.class);

        // 同步 offset 信息
        List<String> queueNames = new ArrayList<>();
        Iterator<Map.Entry<String, QueueInfo>> iterator = BrokerLocalCache.queueInfoMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> next = iterator.next();
            queueNames.add(next.getKey());
        }

        // 设置 offset
        for (String queueName : queueNames) {
            // 缓存到本地中
            String offset = String.valueOf(
                    AdminServerCache.clusterInfo
                            .getQueueOffsetMap()
                            .getOrDefault(queueName, -1l)
            );

            SyncUtil.saveQueueInfoMap(queueName, offset);
        }
    }

    /**
     * 客户端(消费者)请求得到 netty 的远程地址
     */
    @PostMapping("get/netty/server/info")
    public ServerInfo getServerInfo(@RequestBody String reqStr) throws InvalidProtocolBufferException {
        detectFollowerJob.detectFollower();

        MQMessage.MQEntity.Builder tempBuilder = MQMessage.MQEntity.newBuilder();
        JsonFormat.parser().merge(reqStr, tempBuilder);
        MQMessage.MQEntity mqEntity = tempBuilder.build();

        // 如果是Leader，那么就直接随机返回一个 MQ server 地址
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            ServerInfo nettyServer = getNettyServer(mqEntity);
            return nettyServer;
        }
        // 如果是 follower，同样请求 Leader 完成
        if (AdminServerCache.clusterRole.equals(ClusterRole.Follower)) {
            return requestLeader(reqStr);
        }
        // 如果是 observer，那么就返回 null
        return null;
    }

    private ServerInfo getNettyServer(MQMessage.MQEntity mqEntity) {
        switch (mqEntity.getMsgType()) {
            // 如果是生产者
            case 1:
            case 2:
                return doGetNettyServer(mqEntity);
            default:
                log.error("error msg type : {}", mqEntity);
                return null;
        }
    }


    /**
     * 同步当前 server 的所有 queue 数据到另一台服务器，
     * 采用 udp 的方式进行传输数据
     */
    @PostMapping("sync/all/queue")
    public String syncAllData(@RequestBody String toKid) {

        if (AdminServerCache.isSyncData) {
            return "0";
        }

        AdminServerCache.isSyncData = true;
        toKid = JSONObject.parseObject(toKid, String.class);
        if (!StringUtils.isBlank(toKid) && toKid.equals(AdminServerCache.kid)) {
            return null;
        } else {
            ServerInfo serverInfo = null;
            for (ServerInfo temp : mqConfig.getServerList()) {
                if (temp.getKid().equals(toKid)) {
                    serverInfo = temp;
                    break;
                }
            }

            if (null != serverInfo) {

                AdminServerCache.clusterInfo.getKidStatusMap().put(toKid, -1);

                String targetHost = serverInfo.getAddress();
                Integer port = serverInfo.getUdpPort();

                // 找到需要同步的队列信息
                Map<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfo = AdminServerCache.clusterInfo.getKidQueueInfo();

                ConcurrentHashMap<String, QueueInfo> fromQueueInfo = kidQueueInfo.get(AdminServerCache.kid);
                Iterator<Map.Entry<String, QueueInfo>> iterator = fromQueueInfo.entrySet().iterator();

                while (iterator.hasNext()) {
                    // 更新 cluster 数据
                    Map.Entry<String, QueueInfo> infoEntry = iterator.next();
                    QueueInfo queueInfo = infoEntry.getValue();
                    SyncDataUtils.syncClusterInfo();
                    ConcurrentHashMap<String, QueueInfo> toKidQueueInfo = AdminServerCache.clusterInfo.getKidQueueInfo().get(toKid);
                    // 同步对方没有的队列内容
                    if (null == toKidQueueInfo || !toKidQueueInfo.containsKey(queueInfo.getName())) {
                        log.info("start sync [ {} ] msg from kid : [ {} ] to kid: [ {} ] ", queueInfo.getName(), AdminServerCache.kid, toKid);

                        Integer type = 0;
                        switch (queueInfo.getType()) {
                            case PERMANENT:
                                type = 1;
                                break;
                            case TEMPORARY:
                                type = 2;
                                break;
                            case DELAY:
                                type = 3;
                                break;
                        }
                        sendData(targetHost, port, queueInfo, type);
                    }
                }
            }
        }
        AdminServerCache.isSyncData = false;
        return "1";
    }


    @PostMapping("clear/overdue/data")
    public void clearHistoryData() {
        Map<String, QueueInfo> queueInfoMap = BrokerLocalCache.queueInfoMap;
        Iterator<Map.Entry<String, QueueInfo>> iterator = queueInfoMap.entrySet().iterator();
        List<String> queueNameList = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> next = iterator.next();
            QueueInfo queueInfo = next.getValue();
            queueNameList.add(queueInfo.getName());
        }
        log.info("start clear cache data...");
        log.info("ready clear queue list : {} ", queueNameList);
        clearCacheData(queueNameList);
        log.info("clear cache data complete");
    }


    private void clearCacheData(List<String> queueList) {
        // 删除文件夹
        String folder = mqConfig.getCacheFolder();
        File file = new File(folder);
        if (file.exists()) {
            FileWriterUtils.deleteDirectory(file);
        }
        for (int i = 0; i < queueList.size(); i++) {

            // 清空 Tcp 信息
            // 清除记录的 queue info
            BrokerLocalCache.queueInfoMap.remove(queueList.get(i));
            // 清除 普通队列和 临时队列
            BrokerLocalCache.queueMap.remove(queueList.get(i));
            // 清除 延时队列
            BrokerLocalCache.delayQueueMap.remove(queueList.get(i));
            // 清除 queue 与 ctx 对应 map
            BrokerLocalCache.queueCtxListMap.remove(queueList.get(i));
            // 清除 回复队列
            BrokerLocalCache.responseQueue.clear();
            // 清除所有的 channel
            BrokerLocalCache.clientChannels.clear();
            // 清除待处理的 future 信息
            BrokerLocalCache.respFutureMap.clear();
            // 清除 ctx 与 queue 对应的 map
            BrokerLocalCache.ctxQueueListMap.clear();
        }
    }


    private BlockingDeque<MQMessage.MQEntity> getNormalQueue(String name) {
        if (BrokerLocalCache.queueMap.get(name) != null) {
            BlockingDeque<MQMessage.MQEntity> queue = new LinkedBlockingDeque<>(BrokerLocalCache.queueMap.get(name));
            if (null != AdminServerCache.syncDelayQueueMap.get(name)) {
                BlockingDeque<MQMessage.MQEntity> temp = AdminServerCache.syncNormalQueueMap.get(name);
                while (!temp.isEmpty()) {
                    queue.offer(temp.poll());
                }
                AdminServerCache.syncNormalQueueMap.put(name, queue);
            } else {
                return queue;
            }
        }
        return AdminServerCache.syncNormalQueueMap.get(name);
    }

    private DelayQueue<DelayMessage> getDelayQueue(String name) {
        if (BrokerLocalCache.delayQueueMap.get(name) != null) {
            DelayQueue<DelayMessage> delayMessages = new DelayQueue<>(BrokerLocalCache.delayQueueMap.get(name));
            if (null != AdminServerCache.syncDelayQueueMap.get(name)) {
                DelayQueue<DelayMessage> temp = AdminServerCache.syncDelayQueueMap.get(name);
                while (!temp.isEmpty()) {
                    delayMessages.offer(temp.poll());
                }
                AdminServerCache.syncDelayQueueMap.put(name, delayMessages);
            } else {
                return delayMessages;
            }
        }
        return AdminServerCache.syncDelayQueueMap.get(name);
    }


    /**
     * 通过请求 leader 获取 netty server 的信息
     */
    private ServerInfo requestLeader(String requestStr) {
        String targetUrl = "http://" + AdminServerCache.leaderInfo.getTargetAddress() + "/mq/manager/get/get/netty/server/info";
        String responseStr = HttpUtil.postRequest(targetUrl, requestStr);
        if (StringUtils.isBlank(responseStr)) {
            return JSONObject.parseObject(responseStr, ServerInfo.class);
        }

        return null;
    }


    private void sendData(String targetHost, Integer port, QueueInfo queueInfo, Integer type) {
        MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
                .setId(String.valueOf(AdminServerCache.clusterInfo.getQueueOffsetMap()
                        .get(queueInfo.getName())))
                .setDateTime(DateUtils.getNowDate())
                .setMsgType(2)
                .setIsAck(true)
                .putQueueInfo(queueInfo.getName(), type)
                .build();
        AdminServerCache.mqudpServer.sendMsg(targetHost, port, mqEntity);
        // 开始同步信息
        if (queueInfo.getType().equals(QueueType.DELAY)) {
            // 取出延时队列的信息

            DelayQueue<DelayMessage> delayMessages = getDelayQueue(queueInfo.getName());
            while (null != delayMessages && !delayMessages.isEmpty()) {
                AdminServerCache.mqudpServer.sendMsg(targetHost, port,
                        delayMessages.poll().getMqEntity());
            }
        } else {
            BlockingDeque<MQMessage.MQEntity> blockingDeque = getNormalQueue(queueInfo.getName());
            while (null != blockingDeque && !blockingDeque.isEmpty()) {
                AdminServerCache.mqudpServer.sendMsg(targetHost, port, blockingDeque.poll());
            }
        }
    }

    private ServerInfo doGetNettyServer(MQMessage.MQEntity mqEntity) {
        ServerInfo nettyServer = null;
        int maxRelated = Integer.MIN_VALUE;


        Map<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfo = AdminServerCache.clusterInfo.getKidQueueInfo();
        Iterator<Map.Entry<String, ConcurrentHashMap<String, QueueInfo>>> kidQueueInfoIterator = kidQueueInfo.entrySet().iterator();
        while (kidQueueInfoIterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<String, QueueInfo>> next = kidQueueInfoIterator.next();
            ConcurrentHashMap<String, QueueInfo> tempQueueInfoMap = next.getValue();
            Iterator<Map.Entry<String, QueueInfo>> iterator = tempQueueInfoMap.entrySet().iterator();

            // 计算当前 Server 与 msg 中记录的 queue 相关度最高的 server
            int relatedNum = 0;
            while (iterator.hasNext()) {
                Map.Entry<String, QueueInfo> infoEntry = iterator.next();
                String queueName = infoEntry.getKey();
                if (mqEntity.getQueueInfoMap().containsKey(queueName)) {
                    relatedNum++;
                }
            }

            // 不断更新 netty server
            if (relatedNum > maxRelated) {
                for (ServerInfo curServer : AdminServerCache.clusterInfo.getCurServers()) {
                    if (curServer.getKid().equals(next.getKey())) {
                        nettyServer = curServer;
                    }
                }
                maxRelated = relatedNum;
            }

            if (maxRelated == mqEntity.getQueueInfoMap().size() && nettyServer != null) {
                return nettyServer;
            }
        }

        if (nettyServer != null) {
            return nettyServer;
        }
        // 如果 server 不包含 consumer 注册的队列，那么就给定一个随机的 netty server
        return getRandomNettyServer();
    }

    private ServerInfo getRandomNettyServer() {
        List<ServerInfo> ans = new ArrayList<>(AdminServerCache.clusterInfo.getCurServers());
        return ans.get(new Random().nextInt(ans.size()));
    }

}

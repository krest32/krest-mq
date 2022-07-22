package com.krest.mq.admin.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.util.FileWriterUtils;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.HttpUtil;
import com.krest.mq.core.utils.SyncUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

@Slf4j
@RestController
@RequestMapping("mq/manager")
public class ManagerController {

    @Autowired
    MqConfig mqConfig;


    @PostMapping("get/leader/info")
    public ServerInfo getLeaderInfo() {
        if (!AdminServerCache.clusterRole.equals(ClusterRole.Observer) || !AdminServerCache.isSelectServer) {
            return AdminServerCache.leaderInfo;
        }
        return null;
    }

    /**
     * 客户端请求得到 netty 的远程地址
     */
    @PostMapping("get/netty/server/info")
    public ServerInfo getServerInfo(@RequestBody String reqStr) throws InvalidProtocolBufferException {
        System.out.println(reqStr);
        MQMessage.MQEntity.Builder tempBuilder = MQMessage.MQEntity.newBuilder();
        JsonFormat.parser().merge(reqStr, tempBuilder);
        MQMessage.MQEntity mqEntity = tempBuilder.build();

        // 如果是Leader，那么就直接随机返回一个 MQ server 地址
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            return randomNettyServer();
        }
        // 如果是 follower，同样请求 Leader 完成
        if (AdminServerCache.clusterRole.equals(ClusterRole.Follower)) {
            return requestLeader(reqStr);
        }
        // 如果是 observer，那么就返回 null
        return null;
    }

    @GetMapping("get/queue/info")
    public ConcurrentHashMap<String, QueueInfo> getQueueInfo() {
        for (Map.Entry<String, QueueInfo> entry : BrokerLocalCache.queueInfoMap.entrySet()) {
            if (StringUtils.isBlank(entry.getValue().getKid())) {
                entry.getValue().setKid(AdminServerCache.kid);
            }
        }
        return BrokerLocalCache.queueInfoMap;
    }


    /**
     * 采用 udp 的方式进行传输数据
     */
    @PostMapping("sync/queue/data")
    public String syncData(@RequestBody String synchInfoJson) {
        SynchInfo synchInfo = JSONObject.parseObject(synchInfoJson, SynchInfo.class);
        // 先新建要同步的队列信息
        MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setDateTime(DateUtils.getNowDate())
                .setMsgType(2)
                .setIsAck(true)
                .putQueueInfo(synchInfo.getQueueName(), synchInfo.getType())
                .build();
        AdminServerCache.mqudpServer.sendMsg(synchInfo.getAddress(), synchInfo.getPort(), mqEntity);

        // 开始同步数据
        if (!synchInfo.getType().equals(3)) {
            // 取出普通队列
            BlockingDeque<MQMessage.MQEntity> blockingDeque = BrokerLocalCache.queueMap.get(synchInfo.getQueueName());
            while (!blockingDeque.isEmpty()) {
                AdminServerCache.mqudpServer.sendMsg(synchInfo.getAddress(), synchInfo.getPort(), blockingDeque.poll());
            }
        } else {
            // 取出延时队列的信息
            DelayQueue<DelayMessage> delayMessages = BrokerLocalCache.delayQueueMap.get(synchInfo.getQueueName());
            while (!delayMessages.isEmpty()) {
                AdminServerCache.mqudpServer.sendMsg(synchInfo.getAddress(), synchInfo.getPort(),
                        delayMessages.poll().getMqEntity());
            }
        }

        return "0";
    }


    /**
     * 采用 udp 的方式进行传输数据
     */
    @PostMapping("sync/all/queue")
    public String syncAllData(@RequestBody String toKid) {
        log.info("receive sync all queue command, to kid : {} ", toKid);
        toKid = JSONObject.parseObject(toKid, String.class);
        ServerInfo serverInfo = null;
        for (ServerInfo temp : mqConfig.getServerList()) {
            if (temp.getKid().equals(toKid)) {
                serverInfo = temp;
                break;
            }
        }

        System.out.println(serverInfo);
        String targetHost = serverInfo.getAddress();
        Integer port = serverInfo.getUdpPort();

        // 找到需要同步的队列信息
        ConcurrentHashMap<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfo = AdminServerCache.clusterInfo.getKidQueueInfo();
        ConcurrentHashMap<String, QueueInfo> toKidQueueInfo = kidQueueInfo.get(toKid);
        ConcurrentHashMap<String, QueueInfo> fromQueueInfo = kidQueueInfo.get(AdminServerCache.kid);
        Iterator<Map.Entry<String, QueueInfo>> iterator = fromQueueInfo.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> infoEntry = iterator.next();
            QueueInfo queueInfo = infoEntry.getValue();
            // 同步对方没有的队列内容
            if (!toKidQueueInfo.containsKey(queueInfo.getName())) {
                log.info("start sync [ {} ] msg ", queueInfo.getName());

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

                MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setDateTime(DateUtils.getNowDate())
                        .setMsgType(2)
                        .setIsAck(true)
                        .putQueueInfo(queueInfo.getName(), type)
                        .build();
                AdminServerCache.mqudpServer.sendMsg(targetHost, port, mqEntity);

                // 开始同步信息
                if (queueInfo.getType().equals(QueueType.DELAY)) {
                    // 取出延时队列的信息
                    DelayQueue<DelayMessage> delayMessages = BrokerLocalCache.delayQueueMap.get(queueInfo.getName());
                    while (!delayMessages.isEmpty()) {
                        AdminServerCache.mqudpServer.sendMsg(targetHost, port,
                                delayMessages.poll().getMqEntity());
                    }
                } else {
                    BlockingDeque<MQMessage.MQEntity> blockingDeque = BrokerLocalCache.queueMap.get(queueInfo.getName());
                    while (!blockingDeque.isEmpty()) {
                        AdminServerCache.mqudpServer.sendMsg(targetHost, port, blockingDeque.poll());
                    }
                }
            }
        }
        return "0";
    }


    @GetMapping("get/queue/info/{queueName}")
    public Integer getQueue(@PathVariable String queueName) {
        return BrokerLocalCache.queueMap.get(queueName).size();
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

        log.info("同步 offset 到本地");
        // 设置 offset
        for (String queueName : queueNames) {
            // 缓存到本地中
            String offset = String.valueOf(
                    AdminServerCache.clusterInfo
                            .getQueueOffsetMap()
                            .getOrDefault(queueName, -1l));
            SyncUtil.saveQueueInfoMap(queueName, offset);

            AdminServerCache.clusterInfo.getKidQueueInfo()
                    .get(AdminServerCache.kid).get(queueName)
                    .setOffset(offset);
        }

    }

    /**
     * todo 清空数据
     */
    @PostMapping("clear/overdue/data")
    public void clearHistoryData(@RequestBody String queueNameListJson) {
        List<String> queueNameList = (List<String>)
                JSONObject.parseObject(queueNameListJson, ArrayList.class);
        log.info("start clear cache data...");
        clearCacheData(queueNameList);
        log.info("clear cache data complete");
    }

    private void clearCacheData(List<String> queueList) {
        // 删除文件夹
        String folder = mqConfig.getCacheFolder();
        for (int i = 0; i < queueList.size(); i++) {
            File file = new File(folder + "\\" + queueList.get(i));
            if (file.exists()) {
                FileWriterUtils.deleteDirectory(file);
            }
            // 清空 Tcp 信息
            // 清除记录的 queue info
            BrokerLocalCache.queueInfoMap.remove(queueList.get(i));
            // 清除 普通队列和 临时队列
            BrokerLocalCache.queueMap.remove(queueList.get(i));
            // 清除 延时队列
            BrokerLocalCache.delayQueueMap.remove(queueList.get(i));
            // 清除 queue 与 ctx 对应 map
            BrokerLocalCache.queueCtxListMap.remove(queueList.get(i));
//            // 清除 回复队列
//            BrokerLocalCache.responseQueue.clear();
//            // 清除所有的 channel
//            BrokerLocalCache.clientChannels.clear();
//            // 清除待处理的 future 信息
//            BrokerLocalCache.respFutureMap.clear();
//            // 清除 ctx 与 queue 对应的 map
//            BrokerLocalCache.ctxQueueListMap.clear();
        }
    }

    private ServerInfo requestLeader(String requestStr) {
        String targetUrl = "http://" + AdminServerCache.leaderInfo.getTargetAddress() + "/mq/managerget/get/netty/server/info";
        String responseStr = HttpUtil.postRequest(targetUrl, requestStr);
        if (StringUtils.isBlank(responseStr)) {
            return JSONObject.parseObject(responseStr, ServerInfo.class);
        }

        return null;
    }

    private ServerInfo randomNettyServer() {
        Random random = new Random();
        int amount = AdminServerCache.curServers.size();
        int bound = random.nextInt(amount);
        Iterator<ServerInfo> iterator = AdminServerCache.curServers.iterator();
        int cnt = 0;
        ServerInfo serverInfo = null;
        while (cnt < bound && iterator.hasNext()) {
            serverInfo = iterator.next();
            cnt++;
        }
        return serverInfo;
    }
}

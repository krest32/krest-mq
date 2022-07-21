package com.krest.mq.admin.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.mq.admin.thread.SynchDataRunnable;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@RestController
@RequestMapping("mq/manager")
public class ManagerController {


    @PostMapping("get/leader/info")
    public ServerInfo getLeaderInfo() {
        if (!AdminServerCache.clusterRole.equals(ClusterRole.Observer)
                || !AdminServerCache.isSelectServer) {
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


    @PostMapping("upload/cluster-info")
    public void uploadClusterInfo(@RequestBody String kid) {
//        ClusterInfo clusterInfo = AdminServerCache.ClusterInfo;
//        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
//            // 先遍历上传的 cluster info,将 上传的 queue info 添加到 集群的配置中
//            for (QueueInfo queueInfo : queueInfos) {
//                String curKid = queueInfo.getKid();
//                String queueName = queueInfo.getName();
//                int amount = clusterInfo.getQueueAmountMap().getOrDefault(queueName, 0);
//                if (amount == 0) {
//                    /**
//                     *   todo 如果等于 0，说明当前队列是原始数据，需要进行备份同步，
//                     */
//                    boolean flag = SynchData(clusterInfo, curKid, queueName);
//                    if (flag) {
//                    }
//                }
//            }
//        }
//        AdminServerCache.ClusterInfo = clusterInfo;
    }


    @PostMapping("synch/queue/data")
    public String sunchData(@RequestBody SynchInfo synchInfo) {
        //todo  生成一个新的客户端，然后发送数据到另一台Server上
        // 0 代表成功 1 代表失败

        return "0";
    }


    private Boolean SynchData(ClusterInfo clusterInfo, String curKid, String queueName) {

        List<String> kids = selectCopyToList(queueName,
                AdminServerCache.ClusterInfo.getDuplicate() - 1, clusterInfo);
        List<Future> ansList = new ArrayList<>();

        for (int i = 0; i < AdminServerCache.ClusterInfo.getDuplicate(); i++) {
            Future<Object[]> submit = LocalExecutor.NormalUseExecutor
                    .submit(new SynchDataRunnable(curKid, kids.get(i), queueName));
            ansList.add(submit);
        }

        // 分析结果
        for (Future future : ansList) {
            try {
                Boolean flag = (Boolean) future.get();
                if (flag) {
                    continue;
                } else {
                    // todo 删除目标队列的 queue, 然后检测目标的 server连接， 备份剩余的 server
                    log.error("同步失败");

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        // 更新 cluster info
        clusterInfo.getQueueAmountMap().put(queueName, kids.size() + 1);

        return true;
    }

    /**
     * 选取需要进行同步的 kid List
     * 默认规则，选择最少队列原则
     */
    private List<String> selectCopyToList(String queueName, Integer amount, ClusterInfo clusterInfo) {
        List<Map.Entry<String, Integer>> kids = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : clusterInfo.getQueueAmountMap().entrySet()) {
            kids.add(entry);
        }

        // 进行升序排序
        kids.sort(Comparator.comparingInt(Map.Entry::getValue));
        List<String> ansList = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            ansList.add(kids.get(i).getKey());
        }
        return ansList;
    }
}

package com.krest.mq.starter.uitls;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.HttpUtil;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.properties.KrestMQProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Random;

@Slf4j
public class ConnectUtil {


    public static ServerInfo nettyInfo;
    public static ServerInfo mqLeader;
    public static MQMessage.MQEntity registerMsg;
    public static IdWorker idWorker;
    public static KrestMQProperties mqConfig;


    /**
     * 设置注册的 Msg
     */
    public static MQMessage.MQEntity registerMsg() {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        return builder.setId(String.valueOf(idWorker.nextId()))
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addQueue(MQNormalConfig.defaultAckQueue)
                .build();
    }

    public static void initSever() {
        // 获取得到 Leader 信息
        mqLeader = ConnectUtil.getLeaderInfo(mqConfig);

        while (mqLeader == null) {
            log.info("等待 3s,  重试获取 Leader 的信息");
            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            mqLeader = ConnectUtil.getLeaderInfo(mqConfig);
        }
        log.info("get leader info ：" + mqLeader);

        // 有 Leader 分配 kid（对应的Netty服务器）
        registerMsg = registerMsg();
        nettyInfo = ConnectUtil.getNettyServerInfo(mqLeader, registerMsg);

        while (nettyInfo == null) {
            System.out.println(nettyInfo);
            log.info("等待 3s,  重试获取 netty server 的信息");
            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            nettyInfo = ConnectUtil.getNettyServerInfo(mqLeader, registerMsg);
        }
        log.info("get nettyServer info ：" + nettyInfo);
    }


    public static ServerInfo getNettyServerInfo(ServerInfo leader, MQMessage.MQEntity mqEntity) {
        String targetUrl = "http://" + leader.getTargetAddress() + "/mq/manager/get/netty/server/info";
        String responseStr = null;

        try {
            System.out.println(JsonFormat.printer().print(mqEntity));
            responseStr = HttpUtil.postRequest(targetUrl, JsonFormat.printer().print(mqEntity));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

        if (!StringUtils.isBlank(responseStr) && !"error".equals(responseStr)) {
            return JSONObject.parseObject(responseStr, ServerInfo.class);
        }
        return null;
    }

    public static ServerInfo getLeaderInfo(KrestMQProperties config) {
        // 随机获取服务
        List<String> server = config.getServer();
        Random random = new Random();
        String serverStr = server.get(random.nextInt(server.size()));
        String[] info = serverStr.split(":");
        ServerInfo serverInfo = new ServerInfo(info[0], Integer.valueOf(info[1]));
        // 获取 Leader
        String targetUrl = "http://" + serverInfo.getTargetAddress() + "/mq/manager/get/leader/info";
        MqRequest request = new MqRequest(targetUrl, null);
        String responseStr = HttpUtil.postRequest(request);
        if (!StringUtils.isBlank(responseStr) && !"error".equals(responseStr)) {
            return JSONObject.parseObject(responseStr, ServerInfo.class);
        }
        return null;
    }

}

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

    public static ServerInfo mqLeader;
    public static IdWorker idWorker;
    public static KrestMQProperties mqConfig;


    // 获取 server 的基本配置信息
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


    }


    public static ServerInfo getNettyServerInfo(ServerInfo leader, MQMessage.MQEntity mqEntity) {
        String targetUrl = "http://" + leader.getTargetAddress() + "/mq/manager/get/netty/server/info";
        String responseStr = null;
        try {
            responseStr = HttpUtil.postRequest(targetUrl, JsonFormat.printer().print(mqEntity));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        if (!StringUtils.isBlank(responseStr) && !"error".equals(responseStr)) {
            return JSONObject.parseObject(responseStr, ServerInfo.class);
        }
        return null;
    }

    /**
     * 轮询获取
     */
    public static ServerInfo getLeaderInfo(KrestMQProperties config) {
        List<String> servers = config.getServer();
        for (String server : servers) {
            String[] info = server.split(":");
            ServerInfo serverInfo = new ServerInfo(info[0], Integer.valueOf(info[1]));
            // 获取 Leader
            String targetUrl = "http://" + serverInfo.getTargetAddress() + "/mq/manager/get/leader/info";
            MqRequest request = new MqRequest(targetUrl, null);
            String responseStr = HttpUtil.postRequest(request);
            if (!StringUtils.isBlank(responseStr) && !"error".equals(responseStr)) {
                return JSONObject.parseObject(responseStr, ServerInfo.class);
            }
        }
        return null;
    }

}

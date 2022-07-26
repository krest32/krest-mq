package com.krest.mq.starter.template;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MqRequest;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.enums.TransferType;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.HttpUtil;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.core.utils.TcpMsgSendUtils;
import com.krest.mq.starter.producer.MQProducerRunnable;
import com.krest.mq.starter.properties.KrestMQProperties;
import com.krest.mq.starter.uitls.ConnectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import sun.nio.cs.ext.MacArabic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@Slf4j
public class KrestMQTemplate {

    MQTCPClient tcpClient;
    MQMessage.MQEntity registerMsg;


    public KrestMQTemplate() {

        ConnectUtil.initSever();
        registerMsg = getRegisterMsg();

        ServerInfo nettyServerInfo
                = ConnectUtil.getNettyServerInfo(ConnectUtil.mqLeader, this.registerMsg);

        if (null != nettyServerInfo) {
            MQProducerRunnable runnable = new MQProducerRunnable(
                    nettyServerInfo.getAddress(), nettyServerInfo.getTcpPort(),
                    ConnectUtil.idWorker, this.tcpClient, this.registerMsg);

            FutureTask<MQTCPClient> futureTask = new FutureTask(runnable);
            Thread t = new Thread(futureTask);
            t.start();

            try {
                this.tcpClient = futureTask.get();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            } catch (ExecutionException e) {
                log.error(e.getMessage(), e);
            }
        } else {
            log.error("producer client error, msg queue does not exist : " + ConnectUtil.mqConfig.getQueue());
        }
    }

    private MQMessage.MQEntity getRegisterMsg() {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        List<String> queues = ConnectUtil.mqConfig.getQueue();
        Map<String, Integer> map = new HashMap<>();
        for (String queue : queues) {
            map.put(queue, 1);
        }

        return builder.setId(String.valueOf(ConnectUtil.idWorker.nextId()))
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addQueue(MQNormalConfig.defaultAckQueue)
                .putAllQueueInfo(map)
                .build();
    }

    /**
     * 普通的发送消息，默认参数：广播模式、非Ack, 可能会丢失消息
     * 1. 消息内容
     * 2. 队列名称
     */
    public void sendMsg(String msg, String queueName) {
        buildMQReturn(msg, queueName, TransferType.BROADCAST, false, 0L);
    }

    /**
     * 普通的 ack 发送消息，默认参数：广播模式, 会一直等到确认 Ack
     * 1. 消息内容
     * 2. 队列名称
     */
    public boolean sendMsg(String msg, String queueName, Boolean isAck) {
        // 尝试发送消息
        return buildMQReturn(msg, queueName, TransferType.BROADCAST, isAck, 0L);
    }


    public boolean sendMsg(String msg, String queueName, TransferType transferType, Boolean isAck, Long timeout) {
        return buildMQReturn(msg, queueName, transferType, isAck, timeout);
    }

    /**
     * 构建消息，并进行发送
     */
    private boolean buildMQReturn(String msg, String queueName, TransferType transferType, Boolean isAck, Long timeout) {
        MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
                .setId(String.valueOf(ConnectUtil.idWorker.nextId()))
                .setMsgType(1) // 消息生产者
                .addQueue(queueName) // 目标队列
                .setMsg(msg) // 消息内容
                .setIsAck(isAck) // 是否 ack 模式
                .setTransferType(transferType.equals(TransferType.POINT) ? 1 : 2) // 消息接收类型 1. 为单点 2. 为广播
                .setTimeout(timeout)    // ack 模式的超时时间
                .setDateTime(DateUtils.getNowDate())
                .build();
        if (isAck) {
            return TcpMsgSendUtils.ackSendMode(this.tcpClient.channel, mqEntity);
        } else {
            return TcpMsgSendUtils.normalSendMode(this.tcpClient.channel, mqEntity);
        }
    }


}

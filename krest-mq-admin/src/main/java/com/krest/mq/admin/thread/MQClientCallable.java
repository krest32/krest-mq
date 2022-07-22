package com.krest.mq.admin.thread;

import com.krest.mq.admin.initialize.MQClientHandler;
import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.Callable;

@Slf4j
public class MQClientCallable implements Callable {


    String host;
    int port;
    MQTCPClient mqtcpClient;


    public MQClientCallable(String host, int port, MQTCPClient mqtcpClient) {
        this.host = host;
        this.port = port;
        this.mqtcpClient = mqtcpClient;
    }


    /**
     * 设置注册的 Msg
     */
    private MQMessage.MQEntity registerMsg() {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        return builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addQueue(MQNormalConfig.defaultAckQueue)
                .build();
    }

    @Override
    public Object call() throws Exception {
        MQMessage.MQEntity registerMsg = registerMsg();
        this.mqtcpClient = new MQTCPClient(this.host, this.port, registerMsg);
        this.mqtcpClient.connect(new MQClientHandler(
                this.mqtcpClient.getInactiveListener(), registerMsg));
        return this.mqtcpClient;
    }
}

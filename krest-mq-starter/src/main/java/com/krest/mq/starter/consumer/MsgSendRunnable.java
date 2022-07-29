package com.krest.mq.starter.consumer;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.TcpMsgSendUtils;
import com.krest.mq.starter.client.MQTCPClient;

import java.util.concurrent.Callable;

public class MsgSendRunnable implements Callable {

    MQTCPClient tcpClient;
    MQMessage.MQEntity mqEntity;


    public MsgSendRunnable(
            MQTCPClient tcpClient,
            MQMessage.MQEntity mqEntity) {
        this.tcpClient = tcpClient;
        this.mqEntity = mqEntity;
    }

    @Override
    public Object call() throws Exception {
        return TcpMsgSendUtils.ackSendMode(this.tcpClient.channel, this.mqEntity);
    }
}

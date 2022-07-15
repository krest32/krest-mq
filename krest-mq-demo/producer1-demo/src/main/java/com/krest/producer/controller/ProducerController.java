package com.krest.producer.controller;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RequestMapping("com/krest/producer")
@RestController
public class ProducerController {

    @Autowired
    IdWorker idWorker;

    @Autowired
    MQTCPClient mqClient;

    @GetMapping("send/{queue}/{msg}/{transfer}")
    public String sendMsg(@PathVariable String queue,
                          @PathVariable String msg,
                          @PathVariable String transfer) throws InterruptedException {

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(String.valueOf(idWorker.nextId()))
                .setIsAck(false)
                .setMsgType(1)
                .addQueue(queue)
                .setMsg(msg)
                .setTransferType(Integer.valueOf(transfer))
                .setDateTime(DateUtils.getNowDate())
                .build();
        mqClient.sendMsg(request);
        return msg;
    }
}

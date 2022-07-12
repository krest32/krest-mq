package com.krest.producer.controller;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RequestMapping("producer")
@RestController
public class ProducerController {

    @Autowired
    MQTCPClient mqClient;

    @GetMapping("send/{queue}/{msg}")
    public String sendMsg(@PathVariable String queue,
                          @PathVariable String msg) throws InterruptedException {

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .addToQueue(queue)
                .setMsg(msg)
                .setDateTime(DateUtils.getNowDate())
                .build();
        mqClient.sendMsg(request);
        return msg;
    }
}

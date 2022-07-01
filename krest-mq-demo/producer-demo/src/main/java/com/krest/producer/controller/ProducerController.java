package com.krest.producer.controller;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.entity.ModuleType;
import com.krest.mq.core.entity.MsgStatus;
import com.krest.mq.core.producer.MQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RequestMapping("producer")
@RestController
public class ProducerController {

    @Autowired
    MQClient mqClient;

    @GetMapping("send/{queue}/{msg}")
    public String sendMsg(@PathVariable String queue,
                          @PathVariable String msg) throws InterruptedException {
        mqClient.sendMsg(new MQEntity(msg, queue, ModuleType.PRODUCER, MsgStatus.SEND_TO_SERVER));
        return msg;
    }
}

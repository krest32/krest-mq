package com.krest.producer.controller;

import com.krest.mq.core.enums.TransferType;
import com.krest.mq.starter.template.KrestMQTemplate;
import com.krest.producer.entity.RequestEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("producer")
public class ProducerController {

    @Autowired
    KrestMQTemplate mqTemplate;


    @GetMapping("hello")
    public String hello() {
        return "hello";
    }

    @PostMapping("sendMsg1")
    public String templateSendMsg1(@RequestBody RequestEntity entity) {

        Long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            mqTemplate.sendMsg(entity.getMsg(), entity.getQueue(),
                    entity.getTransferType() == 1 ? TransferType.POINT : TransferType.BROADCAST,
                    entity.getIsAck() == 1 ? true : false, entity.getTimeout());

        }
        Long end = System.currentTimeMillis();
        Long ans = (end - start) ;
        return String.valueOf(ans);
    }

    @PostMapping("sendMsg2")
    public String templateSendMsg2(@RequestBody RequestEntity entity) {

        Long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            mqTemplate.sendMsg(entity.getMsg(), entity.getQueue(),
                    entity.getTransferType() == 1 ? TransferType.POINT : TransferType.BROADCAST,
                    entity.getIsAck() == 1 ? true : false, entity.getTimeout());

        }
        Long end = System.currentTimeMillis();
        Long ans = (end - start) ;
        return String.valueOf(ans);
    }
}

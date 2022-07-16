package com.krest.consumer.controller;

import com.krest.mq.starter.common.KrestMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("test")
public class Controller {

    @Autowired
    KrestMQTemplate mqTemplate;


    @GetMapping("templateSendMsg/{queue}/{msg}/{transfer}/{timeout}")
    public String templateSendMsg(@PathVariable String queue,
                                  @PathVariable String msg,
                                  @PathVariable String transfer,
                                  @PathVariable String timeout) throws Throwable {

        mqTemplate.sendMsg(msg, queue, false);
        return msg;
    }
}

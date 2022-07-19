package com.krest.mq.admin.controller;

import com.krest.mq.admin.properties.MqConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("mq/server")
public class Controller {

    @Autowired
    MqConfig mqConfig;

    @GetMapping("config")
    public String getConfig() {
        return mqConfig.toString();
    }
}

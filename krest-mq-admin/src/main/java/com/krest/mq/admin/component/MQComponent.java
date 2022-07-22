package com.krest.mq.admin.component;

import com.krest.mq.admin.properties.MqConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MQComponent {

    @Autowired
    MqConfig config;

}

package com.krest.mq.admin.component;

import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.runnable.UdpServerRunnable;
import com.krest.mq.core.server.MQUDPServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class MQComponent {

    @Autowired
    MqConfig config;

}

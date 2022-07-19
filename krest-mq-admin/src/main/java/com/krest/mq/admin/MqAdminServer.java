package com.krest.mq.admin;

import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;


@Slf4j
@SpringBootApplication
@EnableConfigurationProperties(MqConfig.class)
public class MqAdminServer {
    public static void main(String[] args) {
        SpringApplication.run(MqAdminServer.class, args);
    }
}

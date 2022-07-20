package com.krest.mq.admin;

import com.krest.mq.admin.properties.MqConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableConfigurationProperties(MqConfig.class)
@EnableScheduling
@EnableAsync/*异步执行定时任务*/
public class MqAdminServer {
    public static void main(String[] args) {
        SpringApplication.run(MqAdminServer.class, args);
    }
}

package com.krest.mq.starter.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "krest.mq")
public class KrestMQProperties {
    int workerId;
    int datacenterId;
    List<String> server;
    List<String> queue;
}

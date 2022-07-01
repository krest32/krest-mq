package com.krest.mq.starter.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "krest.mq")
public class KrestMQProperties {
    String host;
    int port;
}

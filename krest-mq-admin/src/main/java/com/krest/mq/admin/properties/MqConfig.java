package com.krest.mq.admin.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ToString
@ConfigurationProperties(prefix = "krest.mq-server")
public class MqConfig {
    // 启动信息
    Integer port;
    String cacheFolder;
    Integer kid;
    Long maxFileSize;
    Integer maxFileCount;
    List<String> cluster;
}

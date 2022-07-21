package com.krest.mq.admin.properties;

import com.krest.mq.core.entity.ServerInfo;
import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

@Data
@ToString
@ConfigurationProperties(prefix = "krest.mq-server")
public class MqConfig {
    String cacheFolder;
    String kid;
    Long maxFileSize;
    Integer duplicate;
    Integer maxFileCount;
    List<ServerInfo> serverList;
}

package com.krest.mq.core.config;

import com.krest.mq.core.entity.ConnType;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MQBuilderConfig {
    boolean pushMode;
    String remoteAddress;
    ConnType connType;
    int remotePort;
    int port;
    int tryTimes;
}

package com.krest.mq.core.config;

import com.krest.mq.core.entity.ConnType;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MQConfig {
    boolean pushMode;
    String remoteAddress;
    ConnType connType;
    int remotePort;
    int port;
    int tryTimes;

}

package com.krest.mq.core.entity;

import lombok.Data;

@Data
public class SynchInfo {
    String address;
    Integer port;
    String queueName;
    Integer type;
    String offset;
}

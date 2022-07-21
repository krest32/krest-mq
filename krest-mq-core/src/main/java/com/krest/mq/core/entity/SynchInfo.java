package com.krest.mq.core.entity;

import lombok.Data;

@Data
public class SynchInfo {
    String fromServer;
    String toServer;
    String queueName;
}

package com.krest.mq.core.entity;

import lombok.Data;

@Data
public class SyncInfo {
    String queueName;
    String mqEntityStr;
}

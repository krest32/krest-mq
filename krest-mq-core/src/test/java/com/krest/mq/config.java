package com.krest.mq;

import com.krest.mq.core.config.MQConfig;

public class config {
    public static void main(String[] args) {
        MQConfig config = MQConfig.builder().pushMode(false).build();
        System.out.println(config.isPushMode());
    }
}

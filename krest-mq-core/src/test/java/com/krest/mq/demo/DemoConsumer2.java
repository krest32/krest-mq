package com.krest.mq.demo;

import com.krest.mq.core.consumer.MQConsumer;

public class DemoConsumer2 {
    public static void main(String[] args) {
        MQConsumer mqConsumer = new MQConsumer("localhost", 8001, "demo2");
        mqConsumer.connect();
    }
}

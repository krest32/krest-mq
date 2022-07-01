package com.krest.mq.demo;

import com.krest.mq.core.producer.MQProducer;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.entity.ModuleType;

import java.util.Date;

public class DemoProducer {

    public static void main(String[] args) throws InterruptedException {
        MQProducer client = new MQProducer("localhost", 8001);
        client.connect();
        for (int i = 0; i < 10; i++) {
            client.sendMsg(new MQEntity(new Date().toString(), "demo1", ModuleType.PRODUCER));
        }
        for (int i = 0; i < 10; i++) {
            client.sendMsg(new MQEntity(new Date().toString(), "demo2", ModuleType.PRODUCER));
        }
//        client.stop();
    }
}

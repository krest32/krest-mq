package com.krest.mq.core.entity;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 延时队列信息
 */
public class DelayMessage implements Delayed {
    long timeout;
    MQMessage.MQEntity mqEntity;


    public DelayMessage(long timeout, MQMessage.MQEntity mqEntity) {
        this.timeout = timeout + System.currentTimeMillis();
        this.mqEntity = mqEntity;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(this.timeout - System.currentTimeMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (getDelay(TimeUnit.MILLISECONDS) > o.getDelay(TimeUnit.MILLISECONDS)) {
            return 1;
        } else {
            return -1;
        }
    }

    public MQMessage.MQEntity getMqEntity() {
        return this.mqEntity;
    }
}

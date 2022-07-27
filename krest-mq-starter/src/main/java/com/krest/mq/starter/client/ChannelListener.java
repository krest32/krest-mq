package com.krest.mq.starter.client;

import com.krest.mq.core.entity.MQMessage;

public interface ChannelListener {
    void onInactive(MQMessage.MQEntity mqEntity) throws InterruptedException;
}

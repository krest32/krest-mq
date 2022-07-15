package com.krest.mq.core.listener;

import com.krest.mq.core.entity.MQMessage;

public interface ChannelListener {
    void onInactive(MQMessage.MQEntity mqEntity) throws InterruptedException;
}

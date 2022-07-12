package com.krest.mq.core.client;

import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.DateUtils;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.NonNull;


public class MQClientFactory {

    MQClient mqClient;

    public MQClientFactory(@NonNull ConnType connType, @NonNull String address,
                           @NonNull int port) {

        if (connType.equals(ConnType.TCP)) {
            mqClient = new MQTCPClient(address, port);
        } else {
            mqClient = new MQUDPClient(address, port);
        }
    }

    public MQClient getClient() {
        return mqClient;
    }
}

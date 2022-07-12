package com.krest.mq.core.server;

import com.krest.mq.core.entity.ConnType;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Data;
import lombok.NonNull;

@Data
public class MQServerFactory {
    private ConnType connType;
    private MQServer mqServer;

    private MQServerFactory() {
    }

    public MQServerFactory(@NonNull ConnType connType, @NonNull int port) {
        this.connType = connType;
        if (connType.equals(ConnType.TCP)) {
            mqServer = new MQTCPServer(port);
        } else {
            mqServer = new MQUDPServer(port);
        }
    }


    public MQServer getServer() {
        return this.mqServer;
    }
}

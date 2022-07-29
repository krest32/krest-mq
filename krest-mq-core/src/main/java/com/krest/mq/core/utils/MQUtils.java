package com.krest.mq.core.utils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQUtils {

    public static Channel tryConnect(Bootstrap bootstrap, String host,
                                     int port) {
        try {
            log.info("connect to netty server [" + host + ":" + port + "].");
            ChannelFuture future = bootstrap.connect(host, port).sync();
            if (future.isSuccess()) {
                log.info("Connect to [" + host + ":" + port + "] successed.");
                Channel channel = future.channel();
                return channel;
            } else {
                log.info("Connect to [" + host + ":" + port + "] failed.");
                return null;
            }
        } catch (Exception exception) {
            log.info("Connect to [" + host + ":" + port + "] failed.");
            return null;
        }
    }
}

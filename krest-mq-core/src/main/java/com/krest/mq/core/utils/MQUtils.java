package com.krest.mq.core.utils;

import com.krest.mq.core.entity.MQEntity;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Slf4j
public class MQUtils {

    public static Channel tryConnect(Bootstrap bootstrap, String host,
                                     int port, MQEntity mqEntity) {
        try {
            log.info("connect to netty server [" + host + ":" + port + "].");
            ChannelFuture future = bootstrap.connect(host, port).sync();
            if (future.isSuccess()) {
                log.info("Connect to [" + host + ":" + port + "] successed.");
                Channel channel = future.channel();
                if (mqEntity != null) {
                    sendMsg(channel, mqEntity);
                }
                return channel;
            } else {
                log.info("Connect to [" + host + ":" + port + "] failed.");
                log.info("Try to reconnect in 10s.");
                Thread.sleep(10000);
                return null;
            }
        } catch (Exception exception) {
            log.info("Connect to [" + host + ":" + port + "] failed.");
            log.info("Try to reconnect in 10 seconds.");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            return null;
        }
    }


    public static void sendMsg(Channel channel, MQEntity entity) {
        try {
            channel.writeAndFlush(entity).sync();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }
}

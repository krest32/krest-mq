package com.krest.mq.core.runnable;

import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.MsgResolver;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.DatagramPacket;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class UdpSendMsgRunnable implements Runnable {

    String queueName;

    public UdpSendMsgRunnable(@NonNull String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void run() {
        if (LocalCache.isPushMode) {
            while (true) {
                try {
                    System.out.println(1);
                    MQMessage.MQEntity mqEntity = LocalCache.queueMap.get(queueName).take();
                    // 等于 2 采用 udp 传输模式
                    CopyOnWriteArraySet<InetSocketAddress> socketAddresses = LocalCache.packetQueueMap.get(queueName);
                    if (null != socketAddresses) {
                        for (InetSocketAddress socket : socketAddresses) {
                            System.out.println(socket);
                            System.out.println(2);
                            if (mqEntity.getIsAck()) {
                                System.out.println(3);
                                boolean flag = ackSendMode(LocalCache.udpChannel, mqEntity, socket);
                                // 如果发送未成功 msg 会被重写写入到 queue 中
                                System.out.println(4);
                                if (!flag) {
                                    replyQueueInfo(mqEntity, queueName);
                                }
                                System.out.println(5);
                            } else {
                                LocalCache.udpChannel.writeAndFlush(MsgResolver.buildUdpDatagramPacket(mqEntity, socket));
                            }
                        }
                        System.out.println(6);
                        // 其他使用 tcp 的传输模式
                    } else {
                        log.info("不存在客户端, 等待15s, 继续执行");
                        Thread.sleep(15 * 1000);

                    }
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            log.error("未知的消息推送模式");
        }

    }

    /**
     * udp ack mode
     */
    private boolean ackSendMode(Channel channel, MQMessage.MQEntity mqEntity, InetSocketAddress socket) {
        int tryCnt = 0;
        while (tryCnt < 3) {
            try {
                ChannelFuture future = channel.writeAndFlush(
                        MsgResolver.buildUdpDatagramPacket(mqEntity, socket));
                future.get(3 * 1000, TimeUnit.MILLISECONDS);
                if (future.isSuccess()) {
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            tryCnt++;
        }
        return false;
    }

    /**
     * 回复 queue 的信息
     */
    private synchronized void replyQueueInfo(MQMessage.MQEntity mqEntity, String queueName) throws InterruptedException {
        BlockingQueue<MQMessage.MQEntity> queue = new LinkedBlockingQueue<>();
        queue.offer(mqEntity);
        while (!LocalCache.queueMap.get(queueName).isEmpty()) {
            LocalCache.queueMap.get(queueName).offer(queue.poll());
        }
    }
}

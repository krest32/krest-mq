package com.krest.mq.core.runnable;

import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

@Slf4j
public class TcpSendMsgRunnable implements Runnable {

    String queueName;

    public TcpSendMsgRunnable(@NonNull String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void run() {
        if (LocalCache.isPushMode) {
            while (true) {
                try {
                    MQMessage.MQEntity mqEntity = LocalCache.queueMap.get(queueName).take();
                    List<Channel> channels = LocalCache.queueCtxListMap.get(queueName);
                    if (null != channels && channels.size() > 0) {
                        // 单点发送
                        if (mqEntity.getTransferType() == 1) {
                            Channel channel = randomChannel(channels);
                            if (mqEntity.getIsAck()) {
                                // ack 机制判断发送模式
                                boolean flag = ackSendMode(channel, mqEntity);
                                // 如果发送未成功 msg 会被重写写入到 queue 中
                                if (!flag) {
                                    replyQueueInfo(mqEntity, queueName);
                                }
                            }
                        } else {
                            for (Channel channel : channels) {
                                if (mqEntity.getIsAck()) {
                                    // ack 机制判断发送模式
                                    boolean flag = ackSendMode(channel, mqEntity);
                                    // 如果发送未成功 msg 会被重写写入到 queue 中
                                    if (!flag) {
                                        replyQueueInfo(mqEntity, queueName);
                                    }
                                } else {
                                    // 普通机制，可能会丢失消息
                                    channel.writeAndFlush(mqEntity);
                                }
                            }
                        }
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

    private Channel randomChannel(List<Channel> channels) {
        return channels.get(new Random().nextInt(channels.size()));
    }

    private boolean ackSendMode(Channel channel, MQMessage.MQEntity mqEntity) {
        int tryCnt = 0;
        while (tryCnt < 3) {
            try {
                ChannelFuture future = channel.writeAndFlush(mqEntity);
                // 等待3s
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
    private synchronized void replyQueueInfo(MQMessage.MQEntity mqEntity, String queueName) throws
            InterruptedException {
        BlockingQueue<MQMessage.MQEntity> queue = new LinkedBlockingQueue<>();
        queue.offer(mqEntity);
        while (!LocalCache.queueMap.get(queueName).isEmpty()) {
            LocalCache.queueMap.get(queueName).offer(queue.poll());
        }
    }
}

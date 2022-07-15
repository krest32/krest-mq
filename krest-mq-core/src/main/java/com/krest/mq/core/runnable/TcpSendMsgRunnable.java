package com.krest.mq.core.runnable;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
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
        while (true) {
            MQMessage.MQEntity mqEntity = null;
            try {
                mqEntity = LocalCache.queueMap.get(queueName).takeFirst();
                List<Channel> channels = LocalCache.queueCtxListMap.get(queueName);
                if (null != channels && channels.size() > 0) {
                    // 开始发送
                    if (sendMsg(mqEntity, channels)) {
                        // 更新本地的缓存的偏移量
                        LocalCache.queueInfoMap.get(queueName).setOffset(mqEntity.getId());
                        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, LocalCache.queueInfoMap);
                    } else {
                        LocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    }
                } else {
                    log.info("不存在客户端, 等待15s, 继续执行");
                    LocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    Thread.sleep(15 * 1000);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.getMessage(), e);
                // 如果在去除消息后发生了异常，仍然需要吧消息返回队列
                if (null != mqEntity) {
                    try {
                        LocalCache.queueMap.get(queueName).putFirst(mqEntity);
                    } catch (InterruptedException interruptedException) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    private boolean sendMsg(MQMessage.MQEntity mqEntity, List<Channel> channels) throws ExecutionException, InterruptedException {
        boolean flag = true;
        // 如果是单点发送
        if (mqEntity.getTransferType() == 1) {
            Channel channel = randomChannel(channels);
            if (mqEntity.getIsAck()) {
                flag = ackSendMode(channel, mqEntity);
            } else {
                flag = normalSendMode(channel, mqEntity);
            }
            // 如果是广播发送
        } else {
            for (Channel channel : channels) {
                if (mqEntity.getIsAck()) {
                    // ack 机制判断发送模式
                    flag = ackSendMode(channel, mqEntity);
                } else {
                    flag = normalSendMode(channel, mqEntity);
                    if (!flag) {
                        return false;
                    }
                }
            }
        }
        return flag;
    }

    private boolean normalSendMode(Channel channel, MQMessage.MQEntity mqEntity) throws ExecutionException, InterruptedException {
        int tryCount = 0;
        while (tryCount < 3) {
            ChannelFuture future = channel.writeAndFlush(mqEntity);
            future.get();
            if (future.isSuccess()) {
                return true;
            }
            tryCount++;
        }
        return false;
    }


    private Channel randomChannel(List<Channel> channels) {
        return channels.get(new Random().nextInt(channels.size()));
    }

    /**
     * ack机制： 发送失败后，会进行重试
     */
    private boolean ackSendMode(Channel channel, MQMessage.MQEntity mqEntity) {
        int tryCnt = 0;
        while (tryCnt < 3) {
            try {
                MQRespFuture respFuture = new MQRespFuture(mqEntity.getId(), 0);
                LocalCache.respFutureHandler.register(mqEntity.getId(), respFuture);
                channel.writeAndFlush(mqEntity);
                if (respFuture.getTimeout() == 0) {
                    respFuture.get();
                } else {
                    respFuture.get(respFuture.getTimeout());
                }
                return true;
                // 发送失败就进行重试
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } catch (Throwable throwable) {
                log.error(throwable.getMessage(), throwable);
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

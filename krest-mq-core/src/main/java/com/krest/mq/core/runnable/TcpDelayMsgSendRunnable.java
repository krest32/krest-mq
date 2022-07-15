package com.krest.mq.core.runnable;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@Slf4j
public class TcpDelayMsgSendRunnable implements Runnable {

    String queueName;

    public TcpDelayMsgSendRunnable(@NonNull String queueName) {
        this.queueName = queueName;
    }

    @Override
    public void run() {
        while (true) {
            DelayMessage delayMessage = null;
            try {
                System.out.println(LocalCache.delayQueueMap);
                System.out.println(queueName);
                System.out.println(LocalCache.delayQueueMap.get(queueName));
                delayMessage = LocalCache.delayQueueMap.get(queueName).take();
                List<Channel> channels = LocalCache.queueCtxListMap.get(queueName);
                if (null != channels && channels.size() > 0) {
                    // 开始发送
                    if (sendMsg(delayMessage.getMqEntity(), channels)) {
                        // 更新本地的缓存的偏移量
                        LocalCache.queueInfoMap.get(queueName).setOffset(delayMessage.getMqEntity().getId());
                        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, LocalCache.queueInfoMap);
                    } else {
                        LocalCache.delayQueueMap.get(queueName).put(delayMessage);
                    }
                } else {
                    log.info("不存在客户端, 等待15s, 继续执行");
                    LocalCache.delayQueueMap.get(queueName).put(delayMessage);
                    Thread.sleep(15 * 1000);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.getMessage(), e);
                // 如果在去除消息后发生了异常，仍然需要吧消息返回队列
                if (null != delayMessage) {
                    LocalCache.delayQueueMap.get(queueName).put(delayMessage);
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
}

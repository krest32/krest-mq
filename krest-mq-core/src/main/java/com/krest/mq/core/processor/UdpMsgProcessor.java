package com.krest.mq.core.processor;

import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.runnable.UdpSendMsgRunnable;
import com.krest.mq.core.utils.MsgResolver;
import com.krest.mq.core.runnable.ExecutorFactory;
import com.krest.mq.core.runnable.ThreadPoolConfig;
import com.krest.mq.core.utils.DateUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


/**
 * 消息处理中心
 */
@Slf4j
public class UdpMsgProcessor {

    static ThreadPoolExecutor sendMsgExecutor = ExecutorFactory.threadPoolExecutor(new ThreadPoolConfig());

    static MQMessage.MQEntity.Builder entityBuilder = MQMessage.MQEntity.newBuilder();

    private static void handlerErr(ChannelHandlerContext ctx, MQMessage.MQEntity entity, String errorMsg) {
        MQMessage.MQEntity response = entityBuilder.setId(entity.getId())
                .setErrFlag(true)
                .setMsg(errorMsg)
                .setDateTime(DateUtils.getNowDate())
                .build();
        ctx.writeAndFlush(response);
    }


    public static void msgCenter(ChannelHandlerContext ctx, DatagramPacket datagramPacket) {
        MQMessage.MQEntity entity = MsgResolver.parseUdpDatagramPacket(datagramPacket);
        // 设置 udp channel
        if (null == LocalCache.udpChannel) {
            LocalCache.udpChannel = ctx.channel();
        }

        System.out.println("服务端获取消息：");
        System.out.println(entity);
        // 先排除异常情况
        // 1. 没有设定消息的来源
        if (entity.getMsgType() == 0) {
            handlerErr(ctx, entity, "unknown msg type");
            return;
        }

        // 2. 生产着设置的消息队列不存在
        if (entity.getMsgType() == 1) {
            ProtocolStringList toQueueList = entity.getQueueList();
            for (String curQueueName : toQueueList) {
                if (LocalCache.queueMap.get(curQueueName) == null) {
                    handlerErr(ctx, entity, "msg queue does not exist!");
                    return;
                }
            }
        }

        // 开始根据消息类型处理消息
        // 1 代表生产则
        // 2 代表消费者
        int msgType = entity.getMsgType();
        switch (msgType) {
            case 1:
                producer(ctx, entity, datagramPacket);
                break;
            case 2:
                consumer(ctx, entity, datagramPacket);
                break;
            default:
                handlerErr(ctx, entity, "unknown msg type");
                break;
        }
    }


    /**
     * 1. 找到对象的 msg queue 放入消息，如果没有就创建新的 msg queue
     * 2. 给生产者返回响应的确认标记
     * 3. 获取对应消费者的ctx，然后推送消息，但是需要判断ctx是否存活状态
     */
    private static void producer(ChannelHandlerContext ctx, MQMessage.MQEntity mqEntity, DatagramPacket packet) {
        // 回复生产者
        if (mqEntity.getIsAck() && mqEntity.getMsg().equals("连接")) {
            MQMessage.MQEntity response = entityBuilder.setId(mqEntity.getId())
                    .setAck(true)
                    .setDateTime(DateUtils.getNowDate())
                    .build();
            System.out.println("返回确认消息");
            ctx.writeAndFlush(MsgResolver.buildUdpDatagramPacket(mqEntity, packet.sender()));
        }


        // 整理消息一次放入到每个消息队列中
        ProtocolStringList queueNames = mqEntity.getQueueList();
        if (!queueNames.isEmpty()) {
            for (String queueName : queueNames) {
                // 将消息放入到队列当中，已经对于 队列不存在的情况作处理，此处不作任何处理
                BlockingQueue<MQMessage.MQEntity> queue = LocalCache.queueMap.get(queueName);
                queue.offer(mqEntity);
            }
        }
    }

    private static void consumer(ChannelHandlerContext ctx, MQMessage.MQEntity request, DatagramPacket packet) {
        // 返回确认信息
        if (request.getIsAck()) {
            ctx.writeAndFlush(new DatagramPacket(
                    Unpooled.copiedBuffer(request.toByteArray()), packet.sender()
            ));
        }

        /**
         * 1. 遍历队列信息
         *      1.1 新队列的添加
         *      1.2 如果是 持久队列，那么需要保存信息到本地
         * 2. 如果是新的队列，就开启推送模式
         */
        Map<String, Integer> queueInfoMap = request.getQueueInfoMap();
        Iterator<Map.Entry<String, Integer>> iterator = queueInfoMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Integer> queueInfo = iterator.next();
            String queueName = queueInfo.getKey();
            int val = queueInfo.getValue();


            // 判断队列是否已经存在
            if (null == LocalCache.queueMap.get(queueName)) {
                LocalCache.queueInfoMap.put(queueName,
                        new QueueInfo(queueName, val == 1 ? QueueType.PERMANENT : QueueType.TEMPORARY));
            } else {
                log.info("队列 [ {} ] 已经存在", queueName);
            }

            // 增加缓存设置
            CopyOnWriteArraySet<InetSocketAddress> datagramPackets =
                    LocalCache.packetQueueMap.getOrDefault(queueName, new CopyOnWriteArraySet<>());
            System.out.println("packet:" + packet);
            datagramPackets.add(packet.sender());
            LocalCache.packetQueueMap.put(queueName, datagramPackets);

            BlockingQueue<MQMessage.MQEntity> curQueue = LocalCache.queueMap.get(queueName);

            if (null == curQueue) {
                curQueue = new LinkedBlockingQueue<>();
                LocalCache.queueMap.put(queueName, curQueue);
                // 开启推送模式
                log.info("队列 [{}] 开始推送", queueName);
                sendMsgExecutor.execute(new UdpSendMsgRunnable(queueName));
            }
        }
    }
}


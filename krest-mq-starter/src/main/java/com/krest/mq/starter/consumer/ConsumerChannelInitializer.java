package com.krest.mq.starter.consumer;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class ConsumerChannelInitializer extends ChannelInitializer {

    ChannelListener inactiveListener;
    Object bean;
    MQMessage.MQEntity mqEntity;

    public ConsumerChannelInitializer(ChannelListener inactiveListener, Object bean, MQMessage.MQEntity mqEntity) {
        this.inactiveListener = inactiveListener;
        this.bean = bean;
        this.mqEntity = mqEntity;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        // 客户端 -> 解码器
        channel.pipeline().addLast(new ProtobufVarint32FrameDecoder());//解决粘包半包编码器
        // 加入一个Decoder
        channel.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
        channel.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());//解决粘包半包编码器
        channel.pipeline().addLast(new ProtobufEncoder());
        channel.pipeline().addLast(new ConsumerHandlerAdapter(inactiveListener, this.bean, this.mqEntity));
    }
}

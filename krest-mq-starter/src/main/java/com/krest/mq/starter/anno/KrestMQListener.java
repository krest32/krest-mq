package com.krest.mq.starter.anno;


import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.entity.TransferType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface KrestMQListener {

    String queue();

    // 默认队列类型： 临时队列
    QueueType queueType() default QueueType.TEMPORARY;

}

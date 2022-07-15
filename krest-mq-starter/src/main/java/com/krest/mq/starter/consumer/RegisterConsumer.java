package com.krest.mq.starter.consumer;

import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.anno.KrestConsumer;
import com.krest.mq.starter.anno.KrestMQListener;
import com.krest.mq.starter.properties.KrestMQProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
@Slf4j
public class RegisterConsumer implements BeanPostProcessor {

    @Autowired
    KrestMQProperties mqProperties;

    @Autowired
    IdWorker idWorker;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // 如果标志了消息队列的注解
        if (bean.getClass().isAnnotationPresent(KrestConsumer.class)) {
            // 获取方法有条件Listener的注解信息
            Class<?> beanClass = bean.getClass();
            Method[] declaredMethods = beanClass.getDeclaredMethods();
            Map<String, Method> queueMethod = new HashMap<>();
            Set<String> queues = new HashSet<>();
            Map<String, Integer> queueInfo = new HashMap<>();
            for (Method curMethod : declaredMethods) {
                if (curMethod.isAnnotationPresent(KrestMQListener.class)) {
                    KrestMQListener listener = curMethod.getAnnotation(KrestMQListener.class);
                    String queueName = listener.queue();
                    QueueType queueType = listener.queueType();
                    int val = 0;
                    switch (queueType) {
                        case PERMANENT:
                            val = 1;
                            break;
                        case TEMPORARY:
                            val = 2;
                            break;
                        case DELAY:
                            val = 3;
                            break;
                    }
                    queueInfo.put(queueName, val);
                    if (queues.add(queueName)) {
                        queueMethod.put(queueName, curMethod);
                    } else {
                        log.error("重复的listener注解");
                    }
                }
            }

            // 新建客户端
            MQConsumerRunnable runnable = new MQConsumerRunnable(
                    mqProperties.getHost(), mqProperties.getPort(), queueInfo, bean, idWorker
            );

            Thread thread = new Thread(runnable);
            thread.start();
        }
        return bean;
    }
}

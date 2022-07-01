package com.krest.mq.starter.consumer;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.starter.anno.KrestConsumer;
import com.krest.mq.starter.anno.KrestMQListener;
import com.krest.mq.starter.properties.KrestMQProperties;
import lombok.SneakyThrows;
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

    Map<String, Method> queueMethod = new HashMap<>();
    Set<String> queues = new HashSet<>();

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

        // 如果标志了消息队列的注解
        if (bean.getClass().isAnnotationPresent(KrestConsumer.class)) {

            // 获取方法有条件Listener的注解信息
            Class<?> beanClass = bean.getClass();
            Method[] declaredMethods = beanClass.getDeclaredMethods();
            for (Method curMethod : declaredMethods) {
                if (curMethod.isAnnotationPresent(KrestMQListener.class)) {
                    KrestMQListener listener = curMethod.getAnnotation(KrestMQListener.class);
                    String queue = listener.queue();
                    if (queues.add(queue)) {
                        queueMethod.put(queue, curMethod);
                    } else {
                        log.error("重复的listener注解");
                    }
                }
            }

            // 新建客户端
            MQConsumerRunnable runnable = new MQConsumerRunnable(
                    mqProperties.getHost(), mqProperties.getPort(), queues, bean
            );
            Thread thread = new Thread(runnable);
            thread.start();

            // 建立 consumer 客户端

        }
        return bean;
    }
}

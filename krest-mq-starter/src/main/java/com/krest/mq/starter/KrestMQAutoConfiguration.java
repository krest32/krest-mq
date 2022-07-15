package com.krest.mq.starter;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.producer.RegisterProducer;
import com.krest.mq.starter.properties.KrestMQProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KrestMQProperties.class)
public class KrestMQAutoConfiguration {

    private KrestMQProperties configProperties;

    public KrestMQAutoConfiguration(KrestMQProperties configProperties) {
        this.configProperties = configProperties;
    }

    private RegisterProducer registerProducer;

    @Bean
    @ConditionalOnMissingBean
    public RegisterProducer getKrestMQService() {
        // 在这个方法中，可以实现注册服务的方法
        this.registerProducer = new RegisterProducer(this.configProperties);
        return this.registerProducer;
    }

    /**
     * 实例化 KrestJobService并载入Spring IoC容器
     */
    @Bean
    @ConditionalOnMissingBean
    public MQTCPClient getMQProducer() {
        // 在这个方法中，可以实现注册服务的方法
        this.registerProducer = new RegisterProducer(this.configProperties);
        return this.registerProducer.getMqProducer();
    }

    @Bean
    @ConditionalOnMissingBean
    public IdWorker getIdWorker() {
        // 在这个方法中，可以实现注册服务的方法
        this.registerProducer = new RegisterProducer(this.configProperties);
        return this.registerProducer.getIdWorker();
    }

}

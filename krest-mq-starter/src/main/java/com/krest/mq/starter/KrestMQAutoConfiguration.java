package com.krest.mq.starter;

import com.krest.mq.core.client.MQTCPClient;
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

    private KrestMQService krestMQService;

    @Bean
    @ConditionalOnMissingBean
    public KrestMQService getKrestMQService() {
        // 在这个方法中，可以实现注册服务的方法
        this.krestMQService = new KrestMQService(this.configProperties);
        return this.krestMQService;
    }

    /**
     * 实例化 KrestJobService并载入Spring IoC容器
     */
    @Bean
    @ConditionalOnMissingBean
    public MQTCPClient getMQProducer() {
        // 在这个方法中，可以实现注册服务的方法
        this.krestMQService = new KrestMQService(this.configProperties);
        return this.krestMQService.getMqProducer();
    }

}

package com.krest.mq.starter;

import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.common.KrestMQTemplate;
import com.krest.mq.starter.properties.KrestMQProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KrestMQProperties.class)
public class KrestMQAutoConfiguration {


    private KrestMQProperties configProperties;
    private KrestMQService mqService;

    public KrestMQAutoConfiguration(KrestMQProperties configProperties) {
        this.configProperties = configProperties;
        this.mqService = new KrestMQService(this.configProperties);
    }


    @Bean
    @ConditionalOnMissingBean
    public IdWorker getIdWorker() {
        return this.mqService.getIdWorker();
    }

    @Bean
    @ConditionalOnMissingBean
    public KrestMQTemplate getMQTemplate() {
        return this.mqService.getMQTemplate();
    }
}

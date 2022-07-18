package com.krest.mq.starter;

import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.common.KrestMQTemplate;
import com.krest.mq.starter.producer.MQProducerRunnable;
import com.krest.mq.starter.properties.KrestMQProperties;

public class KrestMQService {

    private KrestMQProperties config;
    private IdWorker idWorker;

    public KrestMQService(KrestMQProperties config) {
        this.config = config;
        this.idWorker = new IdWorker(
                config.getWorkerId(), config.getDatacenterId(), 1);

    }

    public IdWorker getIdWorker() {
        return this.idWorker;
    }

    public KrestMQTemplate getMQTemplate() {
        return new KrestMQTemplate(this.idWorker,this.config);
    }
}

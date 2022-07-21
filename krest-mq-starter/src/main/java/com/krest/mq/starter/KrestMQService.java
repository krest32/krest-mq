package com.krest.mq.starter;

import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.template.KrestMQTemplate;
import com.krest.mq.starter.properties.KrestMQProperties;
import com.krest.mq.starter.uitls.ConnectUtil;

public class KrestMQService {


    public KrestMQService(KrestMQProperties config) {
        ConnectUtil.mqConfig = config;
        ConnectUtil.idWorker = new IdWorker(
                config.getWorkerId(), config.getDatacenterId(), 1);

    }

    public KrestMQTemplate getMQTemplate() {
        return new KrestMQTemplate();
    }
}

package com.krest.mq.admin.initialize;

import com.krest.file.entity.KrestFileConfig;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.TCPServerRunnable;
import com.krest.mq.core.cache.CacheFileConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MQInitialize implements InitializingBean {

    @Autowired
    MqConfig config;

    @Override
    public void afterPropertiesSet() throws Exception {
        // 设置初始化参数
        CacheFileConfig.queueInfoFilePath = config.getCacheFolder() + "queue-info";
        CacheFileConfig.queueCacheDatePath = config.getCacheFolder();

        // MQ server 缓存文件配置
        KrestFileConfig.maxFileSize = config.getMaxFileSize();
        KrestFileConfig.maxFileCount = config.getMaxFileCount();

        // 启动 mq tcp server
        TCPServerRunnable runnable = new TCPServerRunnable(config.getPort());
        Thread t = new Thread(runnable);
        t.start();
    }
}

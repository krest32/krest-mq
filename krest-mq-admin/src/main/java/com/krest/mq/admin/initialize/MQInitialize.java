package com.krest.mq.admin.initialize;

import com.google.protobuf.FieldOrBuilder;
import com.krest.file.entity.KrestFileConfig;
import com.krest.mq.admin.cache.AdminCache;
import com.krest.mq.admin.entity.MqRequest;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.thread.TCPServerRunnable;
import com.krest.mq.admin.util.HttpUtil;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.NameServerCache;
import com.krest.mq.core.entity.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.exeutor.LocalExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

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
        AdminCache.kid = config.getKid();

        // MQ server 缓存文件配置
        KrestFileConfig.maxFileSize = config.getMaxFileSize();
        KrestFileConfig.maxFileCount = config.getMaxFileCount();

        // 启动 mq tcp server, 开始工作
        TCPServerRunnable runnable = new TCPServerRunnable(config.getPort());
        Thread t = new Thread(runnable);
        t.start();

        // 查找 leader
        LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(config));

    }


}

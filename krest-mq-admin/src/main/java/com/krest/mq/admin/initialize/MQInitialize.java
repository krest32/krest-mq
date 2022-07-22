package com.krest.mq.admin.initialize;

import com.krest.file.entity.KrestFileConfig;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.thread.TCPServerRunnable;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.runnable.UdpServerStartRunnable;
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
        AdminServerCache.kid = config.getKid();
        AdminServerCache.clusterInfo.setDuplicate(config.getDuplicate());
        SyncDataUtils.mqConfig = config;


        // MQ server 缓存文件配置
        KrestFileConfig.maxFileSize = config.getMaxFileSize();
        KrestFileConfig.maxFileCount = config.getMaxFileCount();

        for (ServerInfo serverInfo : config.getServerList()) {
            if (serverInfo.getKid().equals(config.getKid())) {
                AdminServerCache.selfServerInfo = serverInfo;
            }
        }

        // 启动 mq tcp server, 开始工作
        TCPServerRunnable runnable = new TCPServerRunnable(
                AdminServerCache.selfServerInfo.getTcpPort()
        );
        Thread tcpThread = new Thread(runnable);
        tcpThread.start();

        UdpServerStartRunnable udpServerStartRunnable = new UdpServerStartRunnable(
                AdminServerCache.selfServerInfo.getUdpPort()
        );
        Thread udpThread = new Thread(udpServerStartRunnable);
        udpThread.start();

        // 查找 leader
        LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(config));

        // 然后开始收集每个服务的 queue 信息
        SyncDataUtils.collectQueueInfo();

    }
}

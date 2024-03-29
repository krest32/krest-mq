package com.krest.mq.admin.initialize;

import com.krest.file.entity.KrestFileConfig;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.TcpServerRunnable;
import com.krest.mq.admin.util.SyncDataUtil;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.runnable.UdpServerStartRunnable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MqInitialize implements InitializingBean {

    @Autowired
    MqConfig config;

    @Override
    public void afterPropertiesSet() throws Exception {
        // 设置初始化参数
        CacheFileConfig.queueInfoFilePath = config.getCacheFolder() + "queue-info";
        CacheFileConfig.queueCacheDatePath = config.getCacheFolder();
        AdminServerCache.kid = config.getKid();
        AdminServerCache.clusterInfo.get().setDuplicate(config.getDuplicate());
        SyncDataUtil.mqConfig = config;
        // MQ server 缓存文件配置
        KrestFileConfig.maxFileSize = config.getMaxFileSize();
        KrestFileConfig.maxFileCount = config.getMaxFileCount();

        for (ServerInfo serverInfo : config.getServerList()) {
            String kid = serverInfo.getKid();
            AdminServerCache.kidServerMap.put(kid, serverInfo);
            if (serverInfo.getKid().equals(config.getKid())) {
                AdminServerCache.selfServerInfo = serverInfo;
            }
        }

        // 启动 mq tcp server, 开始工作
        TcpServerRunnable runnable = new TcpServerRunnable(
                AdminServerCache.selfServerInfo.getTcpPort()
        );
        Thread tcpThread = new Thread(runnable);
        tcpThread.start();

        UdpServerStartRunnable udpServerStartRunnable = new UdpServerStartRunnable(
                AdminServerCache.selfServerInfo.getUdpPort()
        );
        Thread udpThread = new Thread(udpServerStartRunnable);
        udpThread.start();
    }
}

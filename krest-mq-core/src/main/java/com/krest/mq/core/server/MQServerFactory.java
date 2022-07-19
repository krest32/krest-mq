package com.krest.mq.core.server;

import com.krest.file.entity.KrestFileConfig;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.NameServerCache;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.entity.RunningMode;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Data
@Slf4j
public class MQServerFactory {

    private MQServer mqServer;

    /**
     * 通过配置文件的方式进行启动
     */
    public MQServerFactory(ConnType connType, Integer port) {
        initConfig();
        if (connType.equals(ConnType.TCP)) {
            mqServer = new MQTCPServer(port);
        } else {
            mqServer = new MQUDPServer(port);
        }
    }

    private void initConfig() {
        // MQ server 缓存配置
        CacheFileConfig.queueInfoFilePath = "E:\\Data\\krest-mq\\queue-info";
        CacheFileConfig.queueCacheDatePath = "E:\\Data\\krest-mq\\queue-data\\";


        // MQ server 缓存文件配置
        KrestFileConfig.maxFileSize = 5 * 1024L;
        KrestFileConfig.maxFileCount = 10;


        // 获取cluster配置
        List<String> servers = new ArrayList<>();
        servers.add("localhost:9001");
        servers.add("localhost:9002");
        servers.add("localhost:9003");
        NameServerCache.servers = servers;
        NameServerCache.runningMode = RunningMode.Cluster;
        NameServerCache.kid = 1;
    }

    public MQServer getServer() {
        return this.mqServer;
    }
}

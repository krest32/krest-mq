package com.krest.mq.core.server;

import com.krest.file.entity.KrestFileConfig;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.config.MQBuilderConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.utils.YmlUtils;
import lombok.Data;

@Data
public class MQServerFactory {
    private MQBuilderConfig mqConfig;
    private MQServer mqServer;


    /**
     * 通过配置文件的方式进行启动
     */
    public MQServerFactory() {
        this.mqConfig = new MQBuilderConfig();
        initConfig();
        if (mqConfig.getConnType().equals(ConnType.TCP)) {
            mqServer = new MQTCPServer(this.mqConfig);
        } else {
            mqServer = new MQUDPServer(this.mqConfig);
        }
    }

    private void initConfig() {
        // MQ server 启动配置
        this.mqConfig.setConnType(
                YmlUtils.getConfigStr("krest.mq-server.conn-type").equals("tcp")
                        ? ConnType.TCP : ConnType.UDP);
        this.mqConfig.setPort(Integer.valueOf(YmlUtils.getConfigStr("krest.mq-server.start-port")));
        this.mqConfig.setPushMode(Boolean.valueOf(YmlUtils.getConfigStr("krest.mq-server.is-push-mode")));


        // MQ server 缓存配置
        CacheFileConfig.enableCache = Boolean.valueOf(YmlUtils.getConfigStr("krest.cache-file.enable-cache"));
        CacheFileConfig.queueInfoFilePath = YmlUtils.getConfigStr("krest.cache-file.queue-info");
        CacheFileConfig.queueCacheDatePath = YmlUtils.getConfigStr("krest.cache-file.queue-data");


        // MQ server 缓存文件配置
        KrestFileConfig.maxFileSize = Long.valueOf(YmlUtils.getConfigStr("krest.cache-file.max-size"));
        KrestFileConfig.maxFileCount = Integer.valueOf(YmlUtils.getConfigStr("krest.cache-file.amount"));

    }


    /**
     * 通过 java config 的方式进行启动
     */
    public MQServerFactory(MQBuilderConfig mqConfig) {

        this.mqConfig = mqConfig;
        if (mqConfig.getConnType().equals(ConnType.TCP)) {
            mqServer = new MQTCPServer(this.mqConfig);
        } else {
            mqServer = new MQUDPServer(this.mqConfig);
        }
    }


    public MQServer getServer() {
        return this.mqServer;
    }
}

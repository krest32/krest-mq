package com.krest.mq.admin;

import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.utils.SyncUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableConfigurationProperties(MqConfig.class)
@EnableScheduling
@EnableAsync
public class MqAdminServer {

    public static void main(String[] args) {

        SpringApplication.run(MqAdminServer.class, args);

        if (SyncDataUtils.mqConfig != null) {
            // 等到服务启动，开始 查找 leader
            LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(SyncDataUtils.mqConfig));
        }
    }
}

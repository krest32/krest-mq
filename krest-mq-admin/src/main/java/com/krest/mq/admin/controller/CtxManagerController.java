package com.krest.mq.admin.controller;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.enums.ClusterRole;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("ctx/manager")
public class CtxManagerController {

    @PostMapping("stop/channels")
    public String stopChannels() {
        try {
            for (Channel clientChannel : BrokerLocalCache.clientChannels) {
                System.out.println(clientChannel);
                clientChannel.close();
            }
            AdminServerCache.clusterRole = ClusterRole.LOOKING;
            return "1";
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "-1";
        }
    }
}

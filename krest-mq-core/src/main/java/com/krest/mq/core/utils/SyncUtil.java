package com.krest.mq.core.utils;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.cache.CacheFileConfig;


public class SyncUtil {
    public static void saveQueueInfoMap(String queueName, String offset, Integer amount) {
        BrokerLocalCache.queueInfoMap.get(queueName).setOffset(offset);
        BrokerLocalCache.queueInfoMap.get(queueName).setAmount(amount);
        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, BrokerLocalCache.queueInfoMap);
    }
}

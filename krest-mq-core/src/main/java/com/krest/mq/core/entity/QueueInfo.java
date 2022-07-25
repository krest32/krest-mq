package com.krest.mq.core.entity;

import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.enums.QueueType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueueInfo implements Serializable {

    private static final long serialVersionUID = 1;

    // 当前 server 的 kid
    String kid;

    // queue name
    String name;

    // 队列类型
    QueueType type;

    // 记录当前 queue 的偏移量， 也就是 msg 的 id
    String offset;

    // 记录当前 queue 的数据量
    Integer amount = 0;


    public Integer getAmount() {
        if (null != type && BrokerLocalCache.queueMap.get(name) != null) {
            if (type.equals(QueueType.DELAY)) {
                return BrokerLocalCache.delayQueueMap.get(name).size();
            } else {
                return BrokerLocalCache.queueMap.get(name).size();
            }
        }
        return 0;
    }
}

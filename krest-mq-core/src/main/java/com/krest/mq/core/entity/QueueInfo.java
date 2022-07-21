package com.krest.mq.core.entity;

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
}

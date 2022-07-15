package com.krest.mq.core.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class QueueInfo implements Serializable {
    private static final long serialVersionUID = 1;
    String name;
    QueueType type;
    // 记录当前 queue 的偏移量， 也就是 msg 的 id
    String offset;
}

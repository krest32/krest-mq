package com.krest.mq.core.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class QueueInfo {
    String name;
    QueueType type;
}

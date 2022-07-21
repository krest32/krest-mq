package com.krest.producer.entity;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class RequestEntity {
    String msg;
    String queue;
    Long timeout;
    Integer transferType;
    Integer isAck;
}

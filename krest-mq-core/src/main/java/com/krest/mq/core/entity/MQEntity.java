package com.krest.mq.core.entity;

import com.krest.mq.core.utils.DateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MQEntity implements Serializable {

    private String msg;

    private Set<String> queueName;

    private String queue;

    private ModuleType moduleType;

    private int msgCount;

    private String date = DateUtils.getNowDate();

    private MsgStatus msgStatus;

    public MQEntity(String msg, Set<String> queueName, ModuleType moduleType, MsgStatus msgStatus) {
        this.msg = msg;
        this.queueName = queueName;
        this.moduleType = moduleType;
        this.msgStatus = msgStatus;
    }

    public MQEntity(String msg, String queueName, ModuleType moduleType, MsgStatus msgStatus) {
        this.msg = msg;
        this.queueName = new HashSet<>();
        this.queueName.add(queueName);
        this.moduleType = moduleType;
        this.msgStatus = msgStatus;
    }

    public MQEntity(String msg, Set<String> queueName, ModuleType moduleType) {
        this.msg = msg;
        this.queueName = queueName;
        this.moduleType = moduleType;
    }

    public MQEntity(String msg, String queueName, ModuleType moduleType) {
        this.msg = msg;
        this.queueName = new HashSet<>();
        this.queueName.add(queueName);
        this.moduleType = moduleType;
    }

    public void setQueueName(String queueName) {
        this.queueName = new HashSet<>();
        this.queueName.add(queueName);
    }
}

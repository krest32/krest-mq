package com.krest.mq.admin.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MqRequest {
    String targetUrl;
    Object requestData;
}

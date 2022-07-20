package com.krest.mq.core.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ServerInfo {
    String address;
    Integer port;

    public String getTargetAddress() {
        return address + ":" + port;
    }
}

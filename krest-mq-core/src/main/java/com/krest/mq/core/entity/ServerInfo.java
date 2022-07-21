package com.krest.mq.core.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ServerInfo {
    String kid;
    String address;
    Integer port;
    Integer tcpPort;
    Integer udpPort;

    public ServerInfo(String address, Integer port) {
        this.address = address;
        this.port = port;
    }

    public ServerInfo(String kid, String address, Integer port) {
        this.address = address;
        this.port = port;
        this.kid = kid;
    }

    public String getTargetAddress() {
        return address + ":" + port;
    }
}

package com.krest.mq.yml;

import com.krest.mq.core.utils.YmlUtils;

public class Dmeo {
    public static void main(String[] args) {
        System.out.println(YmlUtils.getConfigArr("server.port"));
    }
}

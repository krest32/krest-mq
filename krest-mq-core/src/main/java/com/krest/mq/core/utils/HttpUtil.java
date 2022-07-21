package com.krest.mq.core.utils;

import com.alibaba.fastjson.JSONObject;


import com.krest.mq.core.entity.MqRequest;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


@Slf4j
public class HttpUtil {

    // 生成 http 请求客户段，同时设置 2s 的超时时间
    static OkHttpClient okHttpClient = new OkHttpClient().newBuilder()
            .connectTimeout(2000, TimeUnit.MILLISECONDS).build();

    /**
     * 发送 post 请求
     */
    public static String postRequest(MqRequest mqRequest) {
        // Josn 格式化请求参数
        RequestBody body = RequestBody.create(
                MediaType.parse("application/json"),
                JSONObject.toJSONString(mqRequest.getRequestData())
        );

        Request request = new Request.Builder()
                .url(mqRequest.getTargetUrl())
                .post(body)
                .build();

        Response response = null;
        try {
            response = okHttpClient.newCall(request).execute();
            return response.body().string();
        } catch (IOException e) {
            log.info("can not connect to : {} ", mqRequest.getTargetUrl());
            return "error";
        } finally {
            if (null != response) {
                response.close();
            }
        }
    }

    /**
     * 发送 post 请求
     */
    public static String postRequest(String targetUrl, String requestJson) {
        // Josn 格式化请求参数
        RequestBody body = RequestBody.create(
                MediaType.parse("application/json"),
                requestJson
        );

        Request request = new Request.Builder()
                .url(targetUrl)
                .post(body)
                .build();

        Response response = null;
        try {
            response = okHttpClient.newCall(request).execute();
            return response.body().string();
        } catch (IOException e) {
            log.info("can not connect to : {} ", targetUrl);
            return "error";
        } finally {
            if (null != response) {
                response.close();
            }
        }
    }

    /**
     * 发送 get 请求
     */
    public static boolean getRequest(MqRequest mqRequest) {
        Request request = new Request.Builder()
                .url(mqRequest.getTargetUrl())
                .get()
                .build();
        try {
            okHttpClient.newCall(request).execute();
            return true;
        } catch (IOException e) {
            log.info("can not connect to : {} ", mqRequest.getTargetUrl());
            return false;
        }
    }


}

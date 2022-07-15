package com.krest.mq.core.utils;

import lombok.Data;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class YmlUtils {

    static Map<String, Object> configMap = new HashMap<>();

    static {
        Yaml yaml = new Yaml();
        String fileName = "application.yml";
        Map<String, Object> ret = (Map<String, Object>) yaml.load(yaml
                .getClass().getClassLoader().getResourceAsStream(fileName));
        configMap = ret;
    }

    /**
     * 自定义Yml文件
     *
     * @param fileName
     */
    public static void setYmlFile(String fileName) {
        Yaml yaml = new Yaml();
        System.out.println(fileName);
        Map<String, Object> ret = (Map<String, Object>) yaml
                .load(yaml.getClass().getClassLoader().getResourceAsStream(fileName));
        configMap = ret;
    }

    /**
     * 获取最终的String值
     *
     * @param configPath
     * @return
     */
    public static String getConfigStr(String configPath) {
        String[] split = configPath.split("\\.");
        Map<String, Object> tempConfig = configMap;
        for (int i = 0; i < split.length; i++) {
            if (i < (split.length - 1)) {
                tempConfig = (Map<String, Object>) tempConfig.get(split[i]);
            }
            if (i == (split.length - 1)) {
                return tempConfig.get(split[i]).toString();
            }
        }
        return null;
    }

    /**
     * 获取配置对象
     *
     * @param configPath
     * @return
     */
    public static Object getConfigObj(String configPath) {
        String[] split = configPath.split("\\.");
        Map<String, Object> tempConfig = configMap;
        for (int i = 0; i < split.length; i++) {
            tempConfig = (Map<String, Object>) tempConfig.get(split[i]);
        }
        return tempConfig;
    }


    /**
     * 获取配置数组
     *
     * @param configPath
     * @return
     */
    public static Object getConfigArr(String configPath) {
        String[] split = configPath.split("\\.");
        List<Object> res = new ArrayList<>();
        Map<String, Object> tempConfig = configMap;
        for (int i = 0; i < split.length; i++) {
            if (i < (split.length - 1)) {
                tempConfig = (Map<String, Object>) tempConfig.get(split[i]);
            } else {
                return tempConfig.get(split[i]);
            }
        }
        return res;
    }
}

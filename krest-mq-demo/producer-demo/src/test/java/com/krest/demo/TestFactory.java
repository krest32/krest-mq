package com.krest.demo;

import java.lang.reflect.Proxy;

public class TestFactory {


    public static class Builder<T> {
        TestEntity testEntity;
        private Class<T> clazz;

        private Builder(Class<T> clazz) {
            this.clazz = clazz;
        }

        /**
         * 构建代理对象
         */
        public T build() {
            testEntity = new TestEntity(0);
            testEntity.getA();
            // 生成代理对象
            return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, testEntity);
        }
    }

    public static <T> Builder<T> create(Class<T> targetClass) {
        return new Builder<T>(targetClass);
    }
}

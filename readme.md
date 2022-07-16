# 项目介绍

## 已完成功能

1. 消费者与生产者对应关系
   1. 一对一
   2. 多对多
2. 消费者与队列关系
   1. 单点消费（随机推送）
   2. 广播模式（集群广播）
3. 消息推送模式：
   1. Server 主动 Push 机制
   2. 基于Ack的 客户端 pull 机制
4. 队列类型
   1. 普通临时队列
   2. 普通持久化队列
   3. 延时（持久化）队列，
5. 延时队列
   1. 每个消息可以指定自己的超时时间
   2. 先过期的消息会优先从队列取出
6. 重试机制
   1. Ack模式下，发送失败后，尝试重新发送，重试上限次数可自定义
   2. 如果消费者不存在，Server会进入等待状态，该等待时间可自定义
   3. 注意：集群广播中，由于单点失败，可能会重复广播，导致某些节点重复消费
7. 基于protobuf二进制，传递消息
8. 数据持久化机制
   1. 消息可持久化
   2. MQ server重启后，消息不丢失
   3. 消费者连接MQ以后，自动推送积压消息
   4. 可设置缓存文件的大小
   5. 持久化机制：同步(暂时不支持异步，异步可能会持久化失败)
   6. 持久化消息超过设定的持久化最大值以后，程序自动覆盖最旧的缓存消息文件
9. 自定义 Spring Starter 整合 Spring Boot
10. 采用Snake.yml作为配置，方便 MQ 进行二次改造（日后可集成Spring Boot）
11. 添加 KrestMQTemplate 用于操作 MQ

## 未来目标
1. 镜像集群模式，实现高可用
2. Cluster集群模式

# MQ-Cluster

## 开篇概念

### Server

1. server 对应 broker
2. 主要作用
   1. 管理 queue 与 broker 列表之间的对应关系
   2. 当某个 broker 宕机，server 负责为 queue 挑选新的 backup ，保证每个 queue 的备份节点都满足最小的集群数
   3. 当新的 broker 加入到节点，
      1. server 会为当前的 broker 添加 queue 信息，
      2. 等到数据同步完成，该 broker 可以进入工作状态
      3. 删除原有 broker 节点中的一部分 queue 的备份量
      4. 保证整个集群中的数据均衡

### broker

1. broker 与 queue 进行对应
2. 主要作用
   1. 对接 client，完成 producer 与 consumer 之间的对应关系
   2. 数据同步，将自己的某一个queue的信息同步给其他的 broker 节点



## 消息生产消费流程

1. producer 同一时间仅维持与其中一个 server 的链接
2. producer  向 server 发送消息
3. server 收到消息，然后根据 queue-- broker之间的关系
4. server 将消息转发给 正在与 consumer 连接的 broker，因为 queue 对应的是多个 broker ，所以会使用广播的模式进行发送
5. 然后由与 consumer 连接的 broker 发送消息
6. 等待消息发送生成功，发送消息的broker同步偏移量给到 queue 对应的其他 broker



## broker 宕机（删除节点）情况

1. 当 broker 宕机，server 检查当前 broker 对应的所有的 queue 信息
2. 如果 broker 的备份数量少于设置的备份数量，那么就开启备份流程



## 备份流程

1. 当前的 broker 新开启一个线程
2. 将 queue 中的信息发送给新的 broker
3. 新的broker在完成同步流程之前，不能够提供消息消费服务
4. 同步流程完成之前，新产生的消息，会以广播的模式被接受
5. 等到消息同步完成，新的broker可以加入到工作节点当中



## kroker 增加节点的情况

1. server 端会从所有的 queue 中挑选几个queue分布到该broker节点上
2. 当设置完成，开启 queue 的信息同步工作
3. 等待同步任务完成，server 会删除冗余的备份
4. 新的节点数据数据同步完成后，可以正式的加入到工作节点当中来






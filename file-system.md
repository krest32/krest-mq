# 文件系统设计

### 索引文件设计

#### 文件内容

最大索引文件

1. 索引 id 范围区域
2. exchange id
3. queue id
4. 文件路径

分区索引文件

1. 当前文件id的范围
2. 当前文件的数据量

目录设计

+ data
  + index file -- 记录 exchange id queue id， 然后找到 queue index file
  + exchange id
    + queue id
      + queue index file -> 文件的起始id - 终止id - 对应的folder id- folder 生成 	时间
      + queue data folder -- 按照每一天生成
        + 记录二进制的数据（每个文件设置行数为5万条左右，如此每天有100个文件生成，一共可以持久化7天的数据量，3500W）

### 写物文件的方式对比

如果操作的是二进制的文件，那么就应该使用带缓冲区的字节流 BufferedOutputStream。

[原文网站](https://blog.csdn.net/zs319428/article/details/119926133?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522165762939916781683913848%2522%252C%2522scm%2522%253A%252220140713.130102334..%2522%257D&request_id=165762939916781683913848&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~sobaiduend~default-1-119926133-null-null.142^v32^pc_search_result_control_group,185^v2^control&utm_term=java%20%E5%86%99%E5%85%A5%E6%96%87%E4%BB%B6&spm=1018.2226.3001.4187)

![image-20220712204048503](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20220712204048503.png)





### 读取超大文件

[4种方案对比：](https://blog.csdn.net/SDDDLLL/article/details/112652597?spm=1001.2101.3001.6650.3&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-3-112652597-blog-120463251.pc_relevant_multi_platform_whitelistv1_exp2&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-3-112652597-blog-120463251.pc_relevant_multi_platform_whitelistv1_exp2&utm_relevant_index=6)

最终选择：Apache Commons IO流

~~~java
LineIterator it = FileUtils.lineIterator(new File(path), "UTF-8");
try {
      while (it.hasNext()) {
          String line = it.nextLine();
      }
} finally {
     LineIterator.closeQuietly(it);
}
~~~



### 删除文件种的特定行 

#### 可以的实现方案

不推荐这么做，因为这样子的话，对于整个系统是比较麻烦的

异步执行

1. 记录删除的日志log
2. 当返回数据时与deletelog核对
3. 异步线程慢慢执行删除任务
4. 等到删除之后，记录删除完成的操作



#### 批量删除（优先这种方案）

设定一个过期时间，批量删除文件内容
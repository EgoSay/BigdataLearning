# Kafka学习

## Introduction📖
>Kafka 基础介绍见: [Kafka基础](https://cjwdream.top/2019/11/18/Kafka%E5%9F%BA%E6%9C%AC%E6%A6%82%E5%BF%B5/)

包含两个模块
- [SpringBoot-Kafka](kafka/springBoot-Kafka) 
    > 结合 SpringBoot 的一个Kafka实战小项目
    
- [Advanced-Kafka](kafka/Advanced-Kafka)
    > Kafka 进阶学习，包括：
    >1. Kafka 自定义提交偏移量
    >2. 再均衡机制
    >3. 拦截器机制
    >4. 消息事务机制
    >....

> 推荐利用 `docker` 安装 雅虎的开源工具 [kafka-manager](https://github.com/yahoo/kafka-manager) 便于监控 Kafka 状态, 简要流程见[主要问题及难点](#主要问题及难点)中说明

## 主要问题及难点  

### 基于 Ambari 部署的 Kafka 环境问题
- 问题描述：在 SpringBoot-Kafka 项目中 生产者 一直写入不了数据
- 原因：查询发现 Kafka 默认配置中 `offsets.topic.replication.factor = 3` 并且 `offsets.commit.required.acks = -1`, 代表需要消息写入全部(三个)副本才算成功，
而我只部署了一个 broker 节点 ，所以一直无法成功写入数据

### docker 安装 雅虎的开源工具 kafka-manager
`kafka-manager` 是雅虎开源的用于 监控 Kafka 的开源工具, 官方说明见[here](https://github.com/yahoo/kafka-manager)
IDEA 中集成 Docker部署

### Kafka 偏移量机制
- 见 [Kafka消息偏移量机制](https://cjwdream.top/2019/11/24/Kafka%E6%B6%88%E6%81%AF%E5%81%8F%E7%A7%BB%E9%87%8F%E6%9C%BA%E5%88%B6/)
- 思考: 

### Kafka消息事务机制

## 参考

### 书籍
- 《Kafka权威指南》

### 博客
- [消息队列热点问题](https://blog.csdn.net/qq_40378034/article/details/98790433)
- [Kafka client 消息接收的三种模式](https://blog.csdn.net/laojiaqi/article/details/79034798)
- [Kafka 0.11.0.0 是如何实现 Exactly-once 语义的](https://www.jianshu.com/p/5d889a67dcd3)
- [Kafka 事务特性分析](https://zhuanlan.zhihu.com/p/42046847)
- [Kafka详解](https://www.jianshu.com/p/d3e963ff8b70)
- [Kafka Clients (At-Most-Once, At-Least-Once, Exactly-Once, and Avro Client)](https://dzone.com/articles/kafka-clients-at-most-once-at-least-once-exactly-o)
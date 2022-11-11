# Godis
## 简介
使用golang实现的简易redis

学习于 [Godis](https://github.com/HDT3213/godis) 项目，主要分分支将Godis进行重构，每个分支代表一个主要特性

## 学习进度：
1. 01_tcp_server：实现redis最基础的tcp服务（22.11.08）
2. 02_redis_protocol_parser：实现redis数据解析协议（22.11.11）。该分支代码能够正常连接redis客户端，但是不能存储数据，因为没有实现内存数据库
3. 03_database：实现redis KV存储（正在实现）

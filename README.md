# Godis
## 简介
使用golang实现的简易redis

学习于 Godis 项目，主要分分支将Godis进行重构，每个分支代表一个主要特性

## 进度
1. 01_tcp_server：实现redis最基础的tcp服务（22.11.08）
2. 02_redis_protocol_parser：实现redis数据解析协议（22.11.11）。该分支代码能够正常连接redis客户端，但是不能存储数据，因为没有实现内存数据库
3. 03_database：实现redis基本数据库框架。当前只实现了简单的ping命令作为测试，之后将加入更多redis命令

## 使用方法
目前仅支持telnet访问：`telnet 127.0.0.1 6399`
之后将支持使用redis-cli访问
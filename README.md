# 04_support_redis_command 分支
## 简介
该分支主要实现各种redis所支持的命令，同时实现各种redis场景的数据结构
同时完善godis的持久化功能，包括aof、rdb

## 时间记录
- 开始：22.11.17

## 实现记录
1. 22.11.18：String数据结构，实现基本的get/set命令
2. 22.11.21 ~ ()：实现aof持久化功能
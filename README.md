# Redis KV存储实现
实现Redis的数据存储功能
原作者博客资料：[Golang 实现 Redis(3): 实现内存数据库](https://www.cnblogs.com/Finley/p/12590718.html)

## 分支主要实现
1. 参考concurrentHashMap的实现，实现并发安全的redis dict数据结构
2. LockMap数据结构，对操作多个key的并发安全问题处理，确保同时操作多个key的时候的并发安全
3. 实现数据库操作接口，让KV项目正确运作起来
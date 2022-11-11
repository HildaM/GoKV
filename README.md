# Redis数据解析协议的实现
让godis支持RESP协议
原作者博客文章：[Golang 实现 Redis(2): 实现 Redis 协议解析器](https://www.cnblogs.com/Finley/p/11923168.html)

核心模块：redis/parser/parser.go

## 进度
当前分支的代码，仅仅实现了redis RESP 协议，项目不能允许，因为没有实现redis内存数据库存储
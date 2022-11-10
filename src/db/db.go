package db

import (
	"Godis/src/db/db"
	"Godis/src/interface/redis"
	"Godis/src/lib/logger"
	"Godis/src/redis/reply"
	"fmt"
	"runtime/debug"
	"strings"
)

// 存储实现相关命令的函数
type CmdFunc func(args [][]byte) redis.Reply

// 存储不同命令的映射
type DB struct {
	cmdMap map[string]CmdFunc
}

func MakeDB() *DB {
	cmdMap := make(map[string]CmdFunc)
	cmdMap["ping"] = db.Ping

	return &DB{
		cmdMap: cmdMap,
	}
}

// 未知错误消息
type UnknownErrReply struct {
}

func (r *UnknownErrReply) ToBytes() []byte {
	return []byte("-Err unknown\r\n")
}

// 处理发送过来的命令
func (db *DB) Exec(args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	cmdFunc, ok := db.cmdMap[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
	}
	if len(args) > 1 {
		// 执行命令
		result = cmdFunc(args[1:])
	} else {
		result = cmdFunc([][]byte{})
	}
	return
}

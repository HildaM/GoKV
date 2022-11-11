package database

import (
	"Godis/database/db"
	"Godis/interface/redis"
	"Godis/redis/protocol"
)

// 存储实现相关命令的函数
type CmdFunc func(args [][]byte) redis.Reply

// 存储不同命令的映射
type DB struct {
	cmdMap map[string]CmdFunc
}

func (db *DB) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	//TODO implement me
	return &protocol.EmptyMultiBulkReply{}
}

func (db *DB) AfterClientClose(c redis.Connection) {
	//TODO implement me
	panic("implement me")
}

func (db *DB) Close() {
	//TODO implement me
	panic("implement me")
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

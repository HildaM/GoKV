package database

import (
	"Godis/config"
	"Godis/interface/redis"
	"Godis/lib/logger"
	"Godis/redis/protocol"
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
)

// MultiDB is a set of multiple database set
type MultiDB struct {
	dbSet []*atomic.Value // *DB

	// TODO
	// handle publish/subscribe
	// hub *pubsub.Hub
	// handle aof persistence
	// aofHandler *aof.Handler

	// store master node address
	//slaveOf     string
	//role        int32
	//replication *replicationStatus
}

// Exec 执行操作数据库命令
// cmdLine example: set key value
func (mdb *MultiDB) Exec(client redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	// 0. 事后处理异常情况
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))

	// TODO 1. auth登录验证
	if cmdName == "auth" {
		return Auth(cmdLine[1:])
	}
	if !isAuthenticated(client) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}

	// TODO 2. 集群命令

	// TODO 3. 无法在集群模式下执行的特殊命令

	// 4. 普通命令
	dbIndex := client.GetDBIndex()
	selectedDB, errReply := mdb.SelectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(client, cmdLine)
}

func isAuthenticated(client redis.Connection) bool {
	return true
}

func Auth(i [][]byte) redis.Reply {
	return nil
}

func (m MultiDB) AfterClientClose(c redis.Connection) {
	//TODO implement me
	panic("implement me")
}

func (m MultiDB) Close() {
	//TODO implement me
	panic("implement me")
}

// SelectDB 返回给定的子数据库
func (mdb *MultiDB) SelectDB(index int) (*DB, *protocol.StandardErrReply) {
	if index >= len(mdb.dbSet) || index < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return mdb.dbSet[index].Load().(*DB), nil
}

// NewStandaloneServer 以单机模式启动godis服务器，同时设置额外的redis功能（发布订阅，主从复制等）
func NewStandaloneServer() *MultiDB {
	mdb := &MultiDB{}

	// 1. 初始化参数
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16 // 默认16个数据库
	}

	// 2. 初始化并创建数据库
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		single := MakeDB()
		single.index = i
		holder := &atomic.Value{}
		holder.Store(single) // 使用atomic原子变量包装，确保并发安全
		mdb.dbSet[i] = holder
	}

	// TODO AOF持久化存储、发布订阅、RDB持久化

	return mdb
}

package database

import (
	"Godis/aof"
	"Godis/config"
	"Godis/interface/database"
	"Godis/interface/redis"
	"Godis/lib/logger"
	"Godis/redis/protocol"
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

// MultiDB is a set of multiple database set
type MultiDB struct {
	dbSet []*atomic.Value // *DB

	// TODO
	// handle publish/subscribe
	// hub *pubsub.Hub

	// handle aof persistence
	aofHandler *aof.Handler

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

	// 3. 无法在集群模式下执行的特殊命令
	if cmdName == "rewriteaof" {
		return RewriteAOF(mdb, cmdLine[1:])
	}

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

	// 3. 初始化AOF相关参数
	validAOF := false
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb, func() database.EmbedDB {
			return MakeBasicMultiDB()
		})
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = aofHandler

		// 为每个子数据库配置aof相关内容
		for _, db := range mdb.dbSet {
			singleDB := db.Load().(*DB)
			singleDB.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}

		validAOF = true
	}

	// 4. RDB持久化
	if config.Properties.RDBFilename != "" && !validAOF {
		// TODO
	}

	return mdb
}

// MakeBasicMultiDB 此数据库仅仅用于aof重写时的备份，或者其他需求。不是主数据库
func MakeBasicMultiDB() database.EmbedDB {
	mdb := &MultiDB{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}

// ForEach traverses all the keys in the given database
func (mdb *MultiDB) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	mdb.mustSelectDB(dbIndex).ForEach(cb)
}

// mustSelectDB 必须成功选择一个db，否则panic
func (mdb *MultiDB) mustSelectDB(index int) *DB {
	selectDB, err := mdb.SelectDB(index)
	if err != nil {
		panic(err)
	}
	return selectDB
}

// RewriteAOF 启动aof重写
func RewriteAOF(db *MultiDB, args [][]byte) redis.Reply {
	err := db.aofHandler.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

package database

import (
	"Godis/datastruct/dict"
	"Godis/datastruct/lock"
	"Godis/interface/redis"
	"Godis/redis/protocol"
	"strings"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

// DB 单个数据库实例
type DB struct {
	// 数据库序号
	index int
	// key --> value
	data dict.Dict
	// key --> expiredTime过期时间
	ttlMap dict.Dict
	// key --> verison版本
	versionMap dict.Dict

	// 某些复杂操作下，需要对多个key上锁，例如（rpush、incr...）
	locker *lock.Locks

	// TODO aof持久化
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

func MakeDB() *DB {
	return &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		locker:     lock.Make(lockerSize),
		// TODO aof参数初始化
	}
}

// Exec 在一个数据库中执行redis命令
// 实例：DB.Exec(nil, utils.ToCmdLine("hstrlen", key, field))
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}

	// 判断是否执行pipeline命令
	//if c != nil && c.InMultiState() {
	//	return EnqueueCmd(c, cmdLine)
	//}

	return db.execNormalCommand(cmdLine)
}

// execNormalCommand 执行普通命令
func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	// 1. 获取命令
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}

	// 2. 命令预处理
	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])

	// 3. 对将要操作的key加锁
	db.addVersion(write...) // 对将要写入的key版本号自增
	db.RWLocks(write, read)
	defer db.RWULocks(write, read)

	// 4. 执行命令
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func EnqueueCmd(c redis.Connection, line [][]byte) redis.Reply {
	return nil
}

func Watch(db *DB, c redis.Connection, i [][]byte) redis.Reply {
	return nil
}

func validateArity(i int, line [][]byte) bool {
	return true
}

func execMulti(db *DB, c redis.Connection) redis.Reply {
	return nil
}

func DiscardMulti(c redis.Connection) redis.Reply {
	return nil
}

func StartMulti(c redis.Connection) redis.Reply {
	return nil
}

/* ------- 数据库并发控制 --------- */

// RWLocks 对读写key上锁
func (db *DB) RWLocks(write []string, read []string) {
	db.locker.RWLocks(write, read)
}

// RWULocks 解锁
func (db *DB) RWULocks(writer []string, read []string) {
	db.locker.RWUnLocks(writer, read)
}

/* ------- redis键值对版本控制 --------- */

// addVersion 版本号自增
func (db *DB) addVersion(keys ...string) {
	// 更新keys的版本号
	for _, key := range keys {
		version := db.GetVersion(key)
		db.versionMap.Put(key, version+1)
	}
}

// GetVersion 返回给定key的版本号
func (db *DB) GetVersion(key string) uint32 {
	version, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return version.(uint32)
}

/* ------- 数据操作接口--------- */

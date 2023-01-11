package database

import (
	"github.com/HildaM/GoKV/datastruct/dict"
	"github.com/HildaM/GoKV/datastruct/lock"
	"github.com/HildaM/GoKV/interface/database"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/redis/protocol"
	"strings"
	"time"
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

	// aof
	addAof func(CmdLine)
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
		addAof:     func(line CmdLine) {},
	}
}

// makeBasicDB 创建一个功能简陋的db，没有并发安全保证
func makeBasicDB() *DB {
	return &DB{
		data:       dict.MakeSimple(),
		ttlMap:     dict.MakeSimple(),
		versionMap: dict.MakeSimple(),
		locker:     lock.Make(1),
		addAof:     func(line CmdLine) {},
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

	// 检查命令参数是否正确
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
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

// execWithLock 在已经上锁场景下执行命令。
// 执行过程中不需要像execNormalCommand那样先执行上锁操作
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknow command: " + cmdName)
	}

	// 参数校验
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	// 执行命令
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// validateArity 检查参数是否正确
// 正数表示必须达到的参数数目，负数表示至少达到的参数数目
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func EnqueueCmd(c redis.Connection, line [][]byte) redis.Reply {
	return nil
}

func Watch(db *DB, c redis.Connection, i [][]byte) redis.Reply {
	return nil
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

/* ------- 数据库操作 --------- */

// RWLocks 对读写key上锁
func (db *DB) RWLocks(write []string, read []string) {
	db.locker.RWLocks(write, read)
}

// RWULocks 解锁
func (db *DB) RWULocks(writer []string, read []string) {
	db.locker.RWUnLocks(writer, read)
}

// GetDataEnity 返回指定key的数据实例
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	val, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}

	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := val.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove 移除指定key
func (db *DB) Remove(key string) {
	db.data.Remove(key)
	db.ttlMap.Remove(key)
	// TODO 原子事务实现
}

// ForEach 遍历数据库，并将数据使用cb函数处理
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	// 调用data自身的foreach遍历
	db.data.ForEach(func(key string, raw interface{}) bool {
		// 将遍历出来的interface转换为DataEntity
		entity, _ := raw.(*database.DataEntity)
		// 获取ttlMap中的过期时间
		var expirationTime *time.Time
		rawExpiredTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime := rawExpiredTime.(time.Time)
			expirationTime = &expireTime
		}
		// 将遍历出来的数据，使用cb函数处理
		return cb(key, entity, expirationTime)
	})
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

/* ------- TTL功能 --------- */

// IsExpired 判断当前key是否过期
func (db *DB) IsExpired(key string) bool {
	rawExpiredTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}

	expiredTime := rawExpiredTime.(time.Time)
	expired := time.Now().After(expiredTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

// Expire 设置过期时间
func (db *DB) Expire(key string, expired time.Time) {
	db.ttlMap.Put(key, expired)
	// TODO 事务处理
}

// Persist 取消key的过期时间
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	// TODO 事务处理
}

// genExpireTask
func genExpireTask(key string) string {
	return "expire:" + key
}

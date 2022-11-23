package database

import (
	"Godis/aof"
	"Godis/interface/database"
	"Godis/interface/redis"
	"Godis/lib/utils"
	"Godis/redis/protocol"
	"strconv"
	"time"
)

const (
	defaultPolicy = iota // default
	insertPolicy         // set nx
	updatePolicy         // set ex
)

const unlimitedTTL int64 = 0

/* ---------- Set命令 ----------*/

// execSet Set命令
func execSet(db *DB, args [][]byte) redis.Reply {
	// 1. 解析数据
	key := string(args[0])
	value := args[1]
	policy := defaultPolicy // default
	ttl := unlimitedTTL     // default

	// TODO 之后会加入setNX，setEX等命令，当前仅仅实现最基本的set功能
	entity := &database.DataEntity{Data: value}

	// 2. 插入数据
	var result int
	switch policy {
	case defaultPolicy:
		db.PutEntity(key, entity)
		result = 1
	case insertPolicy:
		result = db.PutIfAbsent(key, entity)
	case updatePolicy:
		result = db.PutIfExists(key, entity)
	}
	// 更新过期时间
	if result > 0 {
		if ttl != unlimitedTTL {
			expired := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.Expire(key, expired)
			// AOF持久化
			db.addAof(CmdLine{
				[]byte("SET"),
				args[0],
				args[1],
			})
			db.addAof(aof.MakeExpireCmd(key, expired).Args)
		} else {
			db.Persist(key)
			// AOF持久化
			db.addAof(utils.ToCmdLine3("set", args...))
		}
	}

	if result > 0 {
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
}

// execSetNX key只有不存在才能加入
func execSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{Data: value}

	result := db.PutIfAbsent(key, entity)
	if result != 0 {
		db.addAof(utils.ToCmdLine3("setnx", args...))
	}
	return protocol.MakeIntReply(int64(result))
}

// execSetEX SETEX mykey 10 "Hello"
func execSetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[2]
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrReply("ERR invalid expire time in setex")
	}
	ttlTime := ttlArg * 1000

	entity := &database.DataEntity{Data: value}

	db.PutEntity(key, entity)
	expiredTime := time.Now().Add(time.Duration(ttlTime) * time.Millisecond)
	db.Expire(key, expiredTime)

	// AOF持久化
	db.addAof(utils.ToCmdLine3("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expiredTime).Args)

	return &protocol.OkReply{}
}

// execPSetEX 与SetEX命令逻辑完全相同，但是使用毫秒入参
func execPSetEX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	value := args[2]

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &protocol.SyntaxErrReply{}
	}
	if ttlArg <= 0 {
		return protocol.MakeErrReply("ERR invalid expire time in setex")
	}

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
	db.Expire(key, expireTime)

	// AOF持久化
	db.addAof(utils.ToCmdLine3("setex", args...))
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)

	return &protocol.OkReply{}
}

// execMSet batch Set command
func execMSet(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}

	n := len(args)
	for i := 0; i+1 < n; i += 2 {
		key := string(args[i])
		value := args[i+1]
		db.PutEntity(key, &database.DataEntity{Data: value})
	}

	// AOF持久化
	db.addAof(utils.ToCmdLine3("mset", args...))

	return &protocol.OkReply{}
}

func prepareMSet(args [][]byte) ([]string, []string) {
	n := len(args) / 2
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys = append(keys, string(args[2*i]))
	}
	return keys, nil
}

// TODO undoMSet
func undoMSet(db *DB, args [][]byte) []CmdLine {
	//writeKeys, _ := prepareMSet(args)
	//return rollbackGivenKeys(db, writeKeys...)
	return nil
}

// execMSetNX 只有当这组key都不存在的时候，才能添加成功
func execMSetNX(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}

	n := len(args) / 2
	keys := make([]string, n)
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = string(args[2*i])
		values[i] = args[2*i+1]
	}

	// 检查是否所有key都不存在
	for _, key := range keys {
		if _, ok := db.GetEntity(key); ok {
			return protocol.MakeIntReply(0)
		}
	}

	for i, key := range keys {
		db.PutEntity(key, &database.DataEntity{Data: values[i]})
	}

	// AOF持久化
	db.addAof(utils.ToCmdLine3("msetnx", args...))

	return protocol.MakeIntReply(1)
}

/* ---------- Get命令 ----------*/

// execGet Get命令
func execGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	val, err := db.getAsString(key)
	if err != nil {
		return err
	}

	if val == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(val)
}

// getAsString 返回字符串
func (db *DB) getAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}

	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return bytes, nil
}

// execMGet 获取多个key
func execMGet(db *DB, args [][]byte) redis.Reply {
	result := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		bytes, err := db.getAsString(string(args[i]))
		if err != nil {
			_, isWrongType := err.(*protocol.WrongTypeErrReply)
			if isWrongType {
				result[i] = nil // 如果查询到非string类型，跳过即可
				continue
			} else {
				return err
			}
		}

		result[i] = bytes
	}
	return protocol.MakeMultiBulkReply(result)
}

func prepareMGet(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

/*------------------------------- 初始化 -------------------------------*/
func init() {
	// Set
	RegisterCommand("Set", execSet, writeFirstKey, rollbackFirstKey, -3, flagWrite)
	RegisterCommand("SetNx", execSetNX, writeFirstKey, rollbackFirstKey, 3, flagWrite)
	RegisterCommand("SetEX", execSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("PSetEX", execPSetEX, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	RegisterCommand("MSet", execMSet, prepareMSet, undoMSet, -3, flagWrite)
	RegisterCommand("MSetNX", execMSetNX, prepareMSet, undoMSet, -3, flagWrite)
	// Get
	RegisterCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly)
	RegisterCommand("MGet", execMGet, prepareMGet, nil, -2, flagReadOnly)
}

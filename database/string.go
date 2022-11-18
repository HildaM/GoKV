package database

import (
	"Godis/interface/database"
	"Godis/interface/redis"
	"Godis/redis/protocol"
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
			// TODO AOF持久化
		} else {
			db.Persist(key)
			// TODO AOF持久化
		}
	}

	if result > 0 {
		return &protocol.OkReply{}
	}
	return &protocol.NullBulkReply{}
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

/*------------------------------- 初始化 -------------------------------*/
func init() {
	RegisterCommand("Set", execSet, writeFirstKey, nil, -3, flagWrite)
	RegisterCommand("Get", execGet, readFirstKey, nil, 2, flagReadOnly)
}

package database

import (
	SortedSet "github.com/HildaM/GoKV/datastruct/sortedset"
	"github.com/HildaM/GoKV/interface/database"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/lib/utils"
	"github.com/HildaM/GoKV/redis/protocol"
	"strconv"
)

// getAsSortedSet 获取跳表数据
func (db *DB) getAsSortedSet(key string) (*SortedSet.SortedSet, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*SortedSet.SortedSet)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return sortedSet, nil
}

// getOrInitSortedSet 懒加载数据
func (db *DB) getOrInitSortedSet(key string) (sortedSet *SortedSet.SortedSet, inited bool, errReply protocol.ErrorReply) {
	sortedSet, errReply = db.getAsSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}

	inited = false
	if sortedSet == nil { // 跳表结构尚未初始化
		sortedSet = SortedSet.Make()
		db.PutEntity(key, &database.DataEntity{ // 将跳表放入数据库中
			Data: sortedSet,
		})
		inited = true
	}

	return sortedSet, inited, nil
}

// zadd key score member
func execZAdd(db *DB, args [][]byte) redis.Reply {
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}

	// 1. 获取args中的数据
	key := string(args[0])
	size := (len(args) - 1) / 2 // 有多少对[key, score]
	elements := make([]*SortedSet.Element, size)
	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}

		elements[i] = &SortedSet.Element{
			Member: member,
			Score:  score,
		}
	}

	// 2. 将数据加入到数据库中
	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	addValue := 0
	for _, e := range elements {
		if sortedSet.Add(e.Member, e.Score) {
			addValue++
		}
	}

	// 3. 更新aof
	db.addAof(utils.ToCmdLine3("zadd", args...))

	return protocol.MakeIntReply(int64(addValue))
}

func undoZAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+2])
	}

	return rollbackZSetFields(db, key, fields...)
}

func init() {
	RegisterCommand("ZAdd", execZAdd, writeFirstKey, undoZAdd, -4, flagWrite)
}

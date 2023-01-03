package database

import (
	"github.com/HildaM/GoKV/lib/utils"
	"strconv"
)

/*
	事务处理工具类
*/

func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func readFirstKey(args [][]byte) ([]string, []string) {
	// assert len(args) > 0
	key := string(args[0])
	return nil, []string{key}
}

func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	//key := string(args[0])
	// return rollbackGivenKeys(db, key)
	return nil
}

/*
ZSet 事务处理工具类
*/
func rollbackZSetFields(db *DB, key string, fields ...string) []CmdLine {
	var undoCmdLines [][][]byte
	zset, err := db.getAsSortedSet(key)
	if err != nil {
		return nil
	}
	if zset == nil {
		undoCmdLines = append(undoCmdLines,
			utils.ToCmdLine("DEL", key), // 删除zset
		)
		return undoCmdLines
	}

	for _, field := range fields {
		elem, ok := zset.Get(field)
		if !ok { // 添加失败，则消除原有的值
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("ZREM", key, field))
		} else {
			// 若已经添加进入zset中，则将其还原为原本的数值
			score := strconv.FormatFloat(elem.Score, 'f', -1, 64)
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("ZADD", key, score, field))
		}
	}

	return undoCmdLines
}

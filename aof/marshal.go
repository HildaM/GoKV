package aof

import (
	"github.com/HildaM/GoKV/interface/database"
	"github.com/HildaM/GoKV/redis/protocol"
	"strconv"
	"time"
)

// EntityToCmd 序列化一个数据库实例为redis命令
func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}

	var cmd *protocol.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
		// TODO 支持更多格式
	}

	return cmd
}

// Set 命令
var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

// Expired 设置过期时间
var pExpireAtBytes = []byte("PEXPIREAT")

func MakeExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}

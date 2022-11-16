package database

import (
	"Godis/interface/redis"
	"Godis/redis/protocol"
)

/*
	pingpong命令
*/

type PongReply struct {
}

func (r *PongReply) ToBytes() []byte {
	return []byte("+PONG\r\n")
}

func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply("\"" + string(args[0]))
	} else {
		return &ArgNumErrReply{}
	}
}

type ArgNumErrReply struct {
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for 'ping' command\r\n")
}

func testSkip(args [][]byte) ([]string, []string) {
	return nil, nil
}

// 初始化
func init() {
	RegisterCommand("ping", Ping, testSkip, nil, 0, 0)
}

package database

import (
	"github.com/HildaM/GoKV/config"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/redis/protocol"
)

/*
	数据库系统命令
*/

// Auth 认证授权
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}

	if config.Properties.RequirePass == "" {
		return protocol.MakeErrReply("ERR client sent AUTH, but no password is set")
	}

	password := string(args[0])
	if config.Properties.RequirePass != password {
		return protocol.MakeErrReply("ERR invalid password")
	}

	c.SetPassword(password)
	return &protocol.OkReply{}
}

// isAuthenticated 确认是否认证通过
func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return config.Properties.RequirePass == c.GetPassword()
}

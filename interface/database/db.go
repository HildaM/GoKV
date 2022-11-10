package database

import (
	"Godis/interface/redis"
)

type DB interface {
	Exec(client redis.Connection, cmdLine [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}

type DataEntity interface {
}

package db

import "Godis/src/interface/redis"

type DB interface {
	Exec([][]byte) redis.Reply
}

type DataEntity interface {
}

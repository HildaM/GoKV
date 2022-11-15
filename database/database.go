package database

import (
	"Godis/interface/redis"
	"sync/atomic"
)

// MultiDB is a set of multiple database set
type MultiDB struct {
	dbSet []*atomic.Value // *DB

	// TODO
	// handle publish/subscribe
	// hub *pubsub.Hub
	// handle aof persistence
	// aofHandler *aof.Handler

	// store master node address
	//slaveOf     string
	//role        int32
	//replication *replicationStatus
}

func (m MultiDB) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	//TODO implement me
	panic("implement me")
}

func (m MultiDB) AfterClientClose(c redis.Connection) {
	//TODO implement me
	panic("implement me")
}

func (m MultiDB) Close() {
	//TODO implement me
	panic("implement me")
}

func NewStandaloneServer() *MultiDB {
	return nil
}

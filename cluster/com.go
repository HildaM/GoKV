package cluster

import (
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/redis/protocol"
)

var defaultRelayImpl = func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply {
	return protocol.MakeOkReply()
}

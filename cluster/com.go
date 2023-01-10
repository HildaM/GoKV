package cluster

import (
	"errors"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/lib/utils"
	"github.com/HildaM/GoKV/redis/client"
	"github.com/HildaM/GoKV/redis/protocol"
	"strconv"
)

// 默认节点路由实现
var defaultRelayImpl = func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply {
	// 如果发送节点就是自己，则立即执行
	if node == cluster.self {
		return cluster.db.Exec(c, cmdLine)
	}

	// 建立与指定节点的连接
	peerClient, err := cluster.getPeerClient(node)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	defer func() {
		// 连接使用完成后要归还到连接池
		_ = cluster.returnPeerClient(node, peerClient)
	}()

	// 将命令发送到指定节点执行
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex()))) // 切换指定的数据库
	return peerClient.Send(cmdLine)
}

// getPeerClient 使用连接池连接集群中指定的节点
func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.nodeConnections[peer]
	if !ok {
		return nil, errors.New("connection pool not found")
	}

	// 从连接池中获取一个连接，连接指定的节点
	raw, err := pool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("ERR connection pool make wrong type")
	}

	return conn, nil
}

// returnPeerClient 连接使用完后，将其归还给连接池
func (cluster *Cluster) returnPeerClient(node string, peerClient *client.Client) error {
	pool, ok := cluster.nodeConnections[node]
	if !ok {
		return errors.New("ERR connection pool not found")
	}
	pool.Put(peerClient)
	return nil
}

// relay 将命令分发到指定的节点执行
func (cluster *Cluster) relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	return cluster.relayImpl(cluster, peer, c, args)
}

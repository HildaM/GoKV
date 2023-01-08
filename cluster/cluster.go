package cluster

import (
	"github.com/HildaM/GoKV/config"
	database2 "github.com/HildaM/GoKV/database"
	"github.com/HildaM/GoKV/datastruct/dict"
	"github.com/HildaM/GoKV/interface/database"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/lib/consistenthash"
	"github.com/HildaM/GoKV/lib/idgenerator"
	"github.com/HildaM/GoKV/lib/pool"
	"github.com/HildaM/GoKV/lib/utils"
	"github.com/HildaM/GoKV/redis/client"
)

const (
	replicas = 4
)

// 如果只有一个节点，则不需要执行tcc事务处理
var allowFastTransaction = true

// PeerPicker 集群节点抽象
type PeerPicker interface {
	AddNode(keys ...string)
	PickNode(key string) string
}

// Cluster
// 集群实例持有一部分数据，同时与其他节点共同完成数据库操作
type Cluster struct {
	self string // 本集群id

	nodes           []string              // 记录其他集群
	peerPickr       PeerPicker            // 集群节点抽象
	nodeConnections map[string]*pool.Pool // 连接池

	db           database.EmbedDB         // 数据库实例
	transactions *dict.SimpleDict         // id --> Transaction 事务处理
	idGenerator  *idgenerator.IDGenerator // 采用雪花算法生成ID

	// use a variable to allow injecting stub for testing
	relayImpl func(cluster *Cluster, node string, c redis.Connection, cmdLine CmdLine) redis.Reply
}

func MakeCluster() *Cluster {
	// 1. 初始化基本变量
	cluster := &Cluster{
		self: config.Properties.Self,

		db:              database2.NewStandaloneServer(),
		transactions:    dict.MakeSimple(),
		peerPickr:       consistenthash.New(replicas, nil),
		nodeConnections: make(map[string]*pool.Pool),

		idGenerator: idgenerator.MakeGenerator(config.Properties.Self),
		relayImpl:   defaultRelayImpl,
	}

	// 2. 获取所有节点信息，生成哈希环
	contains := make(map[string]struct{})
	nodes := make([]string, 0, len(config.Properties.Peers)+1) // 还要加上自己，所以要加一
	for _, peer := range config.Properties.Peers {
		if _, ok := contains[peer]; ok {
			continue
		}
		contains[peer] = struct{}{}
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self) // 记得加上自己
	cluster.peerPickr.AddNode(nodes...)           // 创建哈希环
	cluster.nodes = nodes

	// 3. 配置集群节点连接池
	connectionPoolConfig := pool.Config{
		MaxIdle:   1,
		MaxActive: 16,
	}
	for _, p := range config.Properties.Peers {
		peer := p
		factory := func() (interface{}, error) {
			c, err := client.MakeClient(peer)
			if err != nil {
				return nil, err
			}

			c.Start() // 建立连接

			// 所有节点都应该使用同一个密码
			if config.Properties.RequirePass != "" {
				// 验证登录
				c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
			}

			return c, nil
		}
		finalizer := func(x interface{}) {
			cli, ok := x.(client.Client)
			if !ok {
				return
			}
			cli.Close()
		}

		// 记录每一个节点的连接
		cluster.nodeConnections[peer] = pool.New(factory, finalizer, connectionPoolConfig)
	}

	return cluster
}

// Close 关闭集群
func (cluster *Cluster) Close() {
	cluster.db.Close()
	for _, node := range cluster.nodeConnections {
		node.Close()
	}
}

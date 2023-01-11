package cluster

import (
	"fmt"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/lib/utils"
	"github.com/HildaM/GoKV/redis/protocol"
	"strconv"
)

/*
	在集群层面实现mset、mget
*/

// MGet 给出key，从集群中获取分布在不同节点的value
func MGet(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'mget' command")
	}

	// 1. 获取keys
	keys := make([]string, len(cmdLine)-1)
	for i := 1; i < len(cmdLine); i++ {
		keys[i-1] = string(cmdLine[i])
	}

	// 2. 从集群中获取values
	resultMap := make(map[string][]byte) // 获取不同节点的返回结果
	groupMap := cluster.groupBy(keys)    // 获取keys在集群上节点的位置分布
	for peer, group := range groupMap {
		resp := cluster.relay(peer, c, utils.ToCmdLine2("MSET", group...))

		// 解析返回响应
		if protocol.IsErrorReply(resp) {
			errReply := resp.(protocol.ErrorReply)
			return protocol.MakeErrReply(
				fmt.Sprintf("ERR during get %s occurs: %v", group[0], errReply.Error()))
		}

		// 记录不同节点的返回结果
		arrReply := resp.(*protocol.MultiBulkReply)
		for i, v := range arrReply.Args {
			key := group[i]
			resultMap[key] = v
		}
	}

	// 3. 返回结果
	result := make([][]byte, len(keys))
	for i, key := range keys {
		result[i] = resultMap[key]
	}

	return protocol.MakeMultiBulkReply(result)
}

// MSet 将节点放入集群中不同的节点中
// 哪个节点调用MSet，则该节点就是整个tcc事务的协调者
func MSet(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	// 检查参数是否正确
	argCount := len(cmdLine) - 1
	if argCount%2 != 0 || argCount < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}

	// 配置参数
	size := argCount / 2 // 键值对数
	keys := make([]string, size)
	valueMap := make(map[string]string)
	for i := 0; i < size; i++ {
		keys[i] = string(cmdLine[2*i+1])
		valueMap[keys[i]] = string(cmdLine[2*i+2])
	}

	groupMap := cluster.groupBy(keys) // 获取keys所在的节点
	if len(groupMap) == 1 && allowFastTransaction {
		// 如果只有一个节点，并且允许不执行事务处理
		for peer := range groupMap {
			cluster.relay(peer, c, cmdLine) // 全部命令都由该节点执行
		}
	}

	// tcc事务 —— try阶段
	var errReply redis.Reply
	txID := cluster.idGenerator.NextID() // 使用雪花算法生成事务ID
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false // 是否回滚标志
	for peer, group := range groupMap {
		peerArgs := []string{txIDStr, "MSET"} // [txIDStr, "MSET", k1, v1, k2, v2.....]
		for _, key := range group {
			peerArgs = append(peerArgs, key, valueMap[key])
		}

		// 将命令请求发送给各个节点
		var resp redis.Reply
		if peer == cluster.self {
			resp = execPrepare(cluster, c, utils.ToCmdLine2("Prepare", peerArgs...))
		} else {
			resp = cluster.relay(peer, c, utils.ToCmdLine2("Prepare", peerArgs...))
		}
		if protocol.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}

	if rollback {
		requestRollback(cluster, c, txID, groupMap)
	} else {
		// 如果所有节点都已经准备就绪，那么可以commit了
		_, errReply = requestCommit(cluster, c, txID, groupMap)
		rollback = errReply != nil
	}

	if !rollback {
		return &protocol.OkReply{}
	}
	return errReply
}

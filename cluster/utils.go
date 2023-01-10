package cluster

/*
	集群工具类
*/

// groupBy 返回集群节点与该节点的key的映射（node --> key）
// 获取keys在集群上节点的位置分布
func (cluster *Cluster) groupBy(keys []string) map[string][]string {
	result := make(map[string][]string)
	for _, key := range keys {
		peer := cluster.peerPickr.PickNode(key)
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key) // 保存peer节点所包含的key
		result[peer] = group
	}

	return result
}

package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

/*
	一致性hash算法实现
*/

// HashFunc 定义生成hashcode的函数
type HashFunc func(data []byte) uint32

// Map 一致性hash算法的哈希环抽象
type Map struct {
	hashFunc HashFunc
	replicas int            // 物理节点产生的虚拟节点数目
	keys     []int          // 哈希环抽象
	hashMap  map[int]string // hashcode（虚拟节点位置）与节点key的映射关系
}

// New 创建哈希环
func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}

	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE // 直接默认使用crc32算法求出hash值
	}
	return m
}

// IsEmpty 判断哈希环是否为空
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// AddNode 将新节点加入到hash环中
func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}

		// 生成节点
		for i := 0; i < m.replicas; i++ { // 生成replicas个虚拟节点
			hashCode := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hashCode) // 加入到哈希环中
			m.hashMap[hashCode] = key         // 记录hashcode与节点key的映射关系，方便查找
		}
	}

	sort.Ints(m.keys) // 确保哈希环始终有序
}

// PickNode 顺时针遍历哈希环，获取最近的节点位置
func (m *Map) PickNode(key string) string {
	if key == "" {
		return ""
	}

	partitionKey := getPartitionKey(key)          // 支持{k1}这种数据查询参数
	hash := int(m.hashFunc([]byte(partitionKey))) // 获取虚拟节点的位置（hashcode）

	// 在哈希环keys上寻找数据 ---> golang 二分搜索：
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash // 寻找第一个比hash大的元素，也就是对应的虚拟节点值
	})

	if idx == len(m.keys) { // 找到末尾也没有找到
		idx = 0
	}

	return m.hashMap[m.keys[idx]] // 返回此hashcode对应的实际物理节点
}

// getPartitionKey 根据key获取指定范围的数据
// support hash tag
func getPartitionKey(key string) string {
	begin := strings.Index(key, "{")
	if begin == -1 { // 不存在{key}的查询参数
		return key
	}

	end := strings.Index(key, "}")
	if end == -1 || end == begin+1 {
		return key
	}

	return key[begin+1 : end] // 转换数据：{1} ----> 1
}

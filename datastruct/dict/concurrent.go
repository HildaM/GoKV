package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

/*
	Note: ConcurrentDict 大量借鉴Java ConcurrentHashMap
*/

// ConcurrentDict 采用分段锁实现的并发安全map
type ConcurrentDict struct {
	table      []*shard // 具体的分片存储
	count      int32    // map的长度
	shardCount int      // 分片数量
}

type shard struct {
	m     map[string]interface{} // 实际数据存储位置
	mutex sync.RWMutex           // 读写锁
}

// computeCapacity 初始化shard容量
func computeCapcity(param int) int {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16

	if n < 0 {
		return 1
	} else if n >= math.MaxInt32 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrent 基于给定的分片（shard）数量，创建一个concurrentDict
func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapcity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: map[string]interface{}{},
		}
	}

	return &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: shardCount,
	}
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 参考Java ConcurrentHashMap 实现spread方法
const HASH_BITS = 0x7fffffff // usable bits of normal node hash

func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	return (hashCode ^ (hashCode >> 16)) & HASH_BITS
}

func (dict *ConcurrentDict) getShard(key uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[key]
}

// Get 获取dict中的元素
func (dict *ConcurrentDict) Get(key string) (val interface{}, exist bool) {
	if dict == nil {
		panic("dict is nil")
	}
	// 1. 获取key对应的分片
	hashCode := fnv32(key)
	shardIndex := dict.spread(hashCode)
	shard := dict.getShard(shardIndex)

	// 2. 获取该分片下的val
	shard.mutex.RLock() // 读锁
	defer shard.mutex.RUnlock()
	val, exist = shard.m[key]
	return
}

// Put 向concurrent中插入数据
func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	// 1. 获取分片
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)

	// 2. 判断是否已经存在
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.m[key]; ok {
		// 直接覆盖
		shard.m[key] = val
		result = 0
	}

	shard.m[key] = val
	result = 1

	// 原子增加
	dict.addCount()
	return
}

// PutIfAbsent 只有key不存在的时候才能添加成功
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}

	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)

	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		result = 0
	}
	shard.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists puts value if the key is exist and returns the number of inserted key-value
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 1
	}
	return 0
}

// addCount 原子增加
func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

// Len 返回dict长度
func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

// Remove removes the key and return the number of deleted key-value
func (dict *ConcurrentDict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		delete(shard.m, key)
		dict.decreaseCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) decreaseCount() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.AddInt32(&dict.count, -1))
}

// ForEach 并发遍历
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, shard := range dict.table {
		shard.mutex.Lock()
		func() {
			defer shard.mutex.Unlock()
			for key, value := range shard.m {
				continues := consumer(key, value)
				if !continues {
					return
				}
			}
		}()
	}
}

// Keys 获取Map所有的key
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})

	return keys
}

// RandomKey returns a key randomly
func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

// RandomKeys 获取limit个随机key，有可能获取到重复元素
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	if limit >= dict.Len() {
		return dict.Keys()
	}

	keys := make([]string, limit)
	shardCount := len(dict.table)
	random := rand.New(rand.NewSource(time.Now().UnixNano())) // 随机数生成器
	for i := 0; i < limit; {
		// 1. 随机获取一个shard
		shard := dict.getShard(uint32(random.Intn(shardCount)))
		if shard == nil {
			continue
		}

		// 2. 随机从shard中获取一个key
		key := shard.RandomKey()
		if key != "" {
			keys[i] = key
			i++ // 只有在成功获取参数后才能自增
		}
	}

	return keys
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		shard := dict.getShard(shardIndex)
		if shard == nil {
			continue
		}
		key := shard.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

func (dict *ConcurrentDict) Clear() {
	// 重新初始化
	*dict = *MakeConcurrent(dict.shardCount)
}

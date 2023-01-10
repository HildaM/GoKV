package idgenerator

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	// epoch0 is set to the twitter snowflake epoch of Nov 04 2010 01:42:54 UTC in milliseconds
	// You may customize this to set a different epoch for your application.
	epoch0      int64 = 1288834974657
	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft))
	timeLeft    uint8 = 22
	nodeLeft    uint8 = 10
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

// IDGenerator 使用雪花算法生成唯一的uint64序列号
type IDGenerator struct {
	mu        *sync.Mutex
	lastStamp int64 // 雪花算法依赖于时间戳实现
	nodeID    int64
	sequence  int64
	epoch     time.Time
}

// MakeGenerator 初始化生成器
func MakeGenerator(node string) *IDGenerator {
	fnv64 := fnv.New64() // fnv hash算法
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

const (
	million = 100000
)

// NextID 获取下一个独一无二ID
// 算法依赖于当前的时间戳
func (gene *IDGenerator) NextID() int64 {
	gene.mu.Lock()
	defer gene.mu.Unlock()

	timeStamp := time.Since(gene.epoch).Nanoseconds() / million
	if timeStamp < gene.lastStamp {
		log.Fatal("ERR can not generate id") // id达到上限
	}

	if timeStamp == gene.lastStamp {
		gene.sequence = (gene.sequence + 1) & maxSequence
		if gene.sequence == 0 {
			for timeStamp <= gene.lastStamp {
				timeStamp = time.Since(gene.epoch).Nanoseconds() / million // 直到当前时间戳大于lastStamp
			}
		}
	} else {
		gene.sequence = 0
	}

	gene.lastStamp = timeStamp

	id := (timeStamp << timeLeft) | (gene.nodeID << nodeLeft) | gene.sequence
	return id
}

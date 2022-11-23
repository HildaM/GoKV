package lock

import (
	"sort"
	"sync"
)

const (
	prime32 = uint32(16777619)
)

type Locks struct {
	table []*sync.RWMutex
}

func Make(size int) *Locks {
	table := make([]*sync.RWMutex, size)
	for i := 0; i < size; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{table: table}
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & uint32(hashCode)
}

// Lock 获取独享锁
func (locks *Locks) Lock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

// RLock 获取共享锁
func (locks *Locks) RLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RLock()
}

// UnLock 释放独享锁
func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

// RUnLock release shared lock
func (locks *Locks) RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}

// toLockIndices 将keys所对应的table_index进行排序
func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]bool)
	// 不直接采用数组方式存储，是因为生成的index不是线性排序的，不能提前预知范围，所以使用map先进行标记
	for _, key := range keys {
		index := locks.spread(fnv32(key))
		indexMap[index] = true
	}

	// 将结果返回
	sortedIndex := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		sortedIndex = append(sortedIndex, index)
	}

	sort.Slice(sortedIndex, func(i, j int) bool {
		if !reverse {
			return sortedIndex[i] < sortedIndex[j]
		}
		return sortedIndex[i] > sortedIndex[j]
	})

	return sortedIndex
}

// Locks 按顺序加锁一组key
// keys可以接收单个string，Locks方法相比于之前的Lock方法更为安全，能避免死锁的发送
func (locks *Locks) Locks(keys ...string) {
	sortedIndexs := locks.toLockIndices(keys, false)
	for _, index := range sortedIndexs {
		locks.table[index].Lock()
	}
}

// RLocks obtains multiple shared locks for reading
// invoking RLock in loop may cause dead lock, please use RLocks
func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RLock()
	}
}

// UnLocks 按顺序解锁一组keys
// keys可以接收单个string，UnLocks方法相比于之前的UnLock方法更为安全，能避免死锁的发送
func (locks *Locks) UnLocks(keys ...string) {
	sortedIndexs := locks.toLockIndices(keys, false)
	for _, index := range sortedIndexs {
		locks.table[index].Unlock()
	}
}

// RUnLocks releases multiple shared locks
func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RUnlock()
	}
}

// RWLocks 同时对读写锁进行加锁
func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	sortedIndexs := locks.toLockIndices(keys, false) // 加锁升序

	// 需要区分读写锁
	writeIndexs := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		index := locks.spread(fnv32(wKey))
		writeIndexs[index] = struct{}{}
	}

	// 加锁
	for _, index := range sortedIndexs {
		if _, ok := writeIndexs[index]; ok {
			// 如果是写锁，则加独享锁
			locks.table[index].Lock()
		} else {
			locks.table[index].RLock()
		}
	}
}

// RWUnLocks unlocks write keys and read keys together. allow duplicate keys
func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, true) // 解锁时逆序，不能设置为false，否则和加锁时的顺序不一致

	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := locks.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}

	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

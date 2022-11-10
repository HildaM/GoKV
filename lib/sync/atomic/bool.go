package atomic

import "sync/atomic"

/*
	对原子变量的封装类
*/

type AtomicBool uint32

func (b *AtomicBool) Get() bool {
	return atomic.LoadUint32((*uint32)((b))) != 0
}

func (b *AtomicBool) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}

package pool

import (
	"sync"
)

type request chan interface{}

// Config 连接池参数
type Config struct {
	MaxIdle   uint // 最大空闲数
	MaxActive uint // 最大连接数据库连接数
}

// Pool 连接池实例，保存godis连接
type Pool struct {
	Config
	factory   func() (interface{}, error) // 创建节点连接
	finalizer func(x interface{})         // 回收连接

	idles       chan interface{} // 空闲连接
	activeCount uint             // 在连接增加的时候增加，在连接减少的时候减少

	waitingReqs []request // 等待获取连接池连接的请求

	mu     sync.Mutex
	closed bool // 连接池关闭标志
}

func New(factory func() (interface{}, error), finalizer func(x interface{}), cfg Config) *Pool {
	return &Pool{
		factory:     factory,
		finalizer:   finalizer,
		Config:      cfg,
		idles:       make(chan interface{}, cfg.MaxIdle),
		waitingReqs: make([]request, 0),
	}
}

// Close 关闭连接池
func (pool *Pool) Close() {
	pool.mu.Lock() // 上锁，防止连接池被操作
	if pool.closed {
		pool.mu.Unlock()
		return
	}

	pool.closed = true
	close(pool.idles)
	pool.mu.Unlock()

	// 回收所有连接
	for node := range pool.idles {
		pool.finalizer(node)
	}
}

package pool

import (
	"errors"
	"github.com/HildaM/GoKV/redis/client"
	"sync"
)

var (
	ErrClosed = errors.New("pool closed")
	ErrMax    = errors.New("reach max connection limit")
)

// Config 连接池参数
type Config struct {
	MaxIdle   uint // 最大空闲数
	MaxActive uint // 最大连接数据库连接数
}

type request chan interface{}

// Pool 连接池实例，保存godis连接
type Pool struct {
	Config
	factory   func() (interface{}, error) // 创建节点连接
	finalizer func(x interface{})         // 回收连接

	idles       chan interface{} // 空闲连接
	activeCount uint             // 在连接增加的时候增加，在连接减少的时候减少

	waitingReqs []request // 等待获取连接池连接的请求（request是一个管道）

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

// Get 获取连接池中空闲连接
func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()

	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}

	select {
	case freeConn := <-pool.idles:
		pool.mu.Unlock()
		return freeConn, nil
	default:
		// 连接池没有连接，需要自己创建
		return pool.getOnNoIdle()
	}
}

// getOnNoIdle 在连接池为空，或者没有连接的情况下，创建连接
func (pool *Pool) getOnNoIdle() (interface{}, error) {
	if pool.activeCount >= pool.MaxActive {
		// 连接数已经达到Max，需要阻塞等待连接归还
		req := make(chan interface{}, 1)
		pool.waitingReqs = append(pool.waitingReqs, req) // 将请求放入等待队列中
		pool.mu.Unlock()                                 // 解除上面Get()函数的锁

		// 阻塞等待
		freeConn, ok := <-req
		if !ok {
			return nil, ErrMax
		}
		return freeConn, nil
	}

	// activeCount < MaxActive，则创建新连接
	pool.activeCount++
	pool.mu.Unlock() // 解除Get的上锁

	newConn, err := pool.factory()
	if err != nil {
		// 无法创建连接
		pool.mu.Lock()
		pool.activeCount--
		pool.mu.Unlock()

		return nil, err
	}

	return newConn, nil
}

// Put 将使用完毕的连接归还给连接池
func (pool *Pool) Put(client *client.Client) {
	pool.mu.Lock()

	if pool.closed {
		pool.mu.Unlock()
		pool.finalizer(client) // 中断此连接
		return
	}

	// 如果此时有等待空闲连接的请求
	if len(pool.waitingReqs) > 0 {
		req := pool.waitingReqs[0]                                    // 先来后到
		copy(pool.waitingReqs, pool.waitingReqs[1:])                  // 弹出队头节点
		pool.waitingReqs = pool.waitingReqs[:len(pool.waitingReqs)-1] // 节点弹出，等待队列长度减一

		req <- client // 将空闲连接发送给等待连接的客户端
		pool.mu.Unlock()
		return
	}

	select {
	case pool.idles <- client:
		pool.mu.Unlock()
		return
	default:
		// idle不能接收新请求，已经达到最大MaxIdle，删除连接
		pool.mu.Unlock()
		pool.activeCount--
		pool.finalizer(client)
	}
}

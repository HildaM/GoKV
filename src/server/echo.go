package server

import (
	"Godis/src/lib/sync/atomic"
	"Godis/src/lib/sync/wait"
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type EchoHandler struct {
	// 存储每个连接
	activeConn sync.Map
	closing    atomic.AtomicBool
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// 客户端
type Client struct {
	// tcp 连接
	Conn net.Conn
	// 当服务端开始发送数据时进入waiting, 阻止其它goroutine关闭连接
	// wait.Wait是作者编写的带有最大等待时间的封装:
	Waiting wait.Wait
}

func (c *Client) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

func (h *EchoHandler) Hanlder(ctx context.Context, conn net.Conn) {
	// 如果不等于0，说明在关闭状态下有请求连接
	if h.closing.Get() {
		conn.Close()
	}

	client := &Client{
		Conn: conn,
	}
	h.activeConn.Store(client, 1)

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("connection close")
				h.activeConn.Delete(conn)
			} else {
				log.Println(err)
			}
			return
		}

		// 使用waitGroup进行并发保护
		client.Waiting.Add(1)
		b := []byte(msg)
		conn.Write(b)
		client.Waiting.Done()
	}
}

func (h *EchoHandler) Close() error {
	log.Println("handler shuting donw.....")
	h.closing.Set(true)

	// 将sync.Map中所有client关闭
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*Client)
		client.Close()
		return true
	})

	return nil
}

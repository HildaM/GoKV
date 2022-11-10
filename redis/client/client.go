package client

import (
	"Godis/lib/sync/atomic"
	"Godis/lib/sync/wait"
	"net"
	"time"
)

type Client struct {
	Conn net.Conn

	// 等待响应处理完成
	waittingReply wait.Wait

	// 并发处理请求锁
	sending atomic.AtomicBool

	// 将要发送的数据。若是多个命令，则减去第一行
	// multi bulk msg lineCount - 1(first line)
	expectedLineCount uint32
	// 已经发送的数据
	sentLineCount uint32
	// sent lines, exclude first line
	sentLines [][]byte
}

func (c *Client) Close() error {
	c.waittingReply.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

func MakeClient(conn net.Conn) *Client {
	return &Client{
		Conn: conn,
	}
}

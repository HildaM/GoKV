package connection

import (
	"Godis/lib/sync/atomic"
	"Godis/lib/sync/wait"
	"net"
	"time"
)

type Connection struct {
	conn net.Conn

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

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.waittingReply.WaitWithTimeout(10 * time.Second)
	c.conn.Close()
	return nil
}

func (c *Connection) Write(i interface{}) interface{} {
	return nil
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

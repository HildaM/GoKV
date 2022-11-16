package connection

import (
	"Godis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection 代表一个redis-cli连接
type Connection struct {
	conn net.Conn

	// 消息等待 —— 让信息完整地发送
	waittingReply wait.Wait

	// 并发锁，确保server完整处理消息
	mu sync.Mutex

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// selected db
	selectedDB int
	role       int32
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.waittingReply.WaitWithTimeout(10 * time.Second)
	c.conn.Close()
	return nil
}

func (c *Connection) Write(msg []byte) error {
	if len(msg) == 0 {
		return nil
	}

	c.waittingReply.Add(1)
	defer c.waittingReply.Done()

	_, err := c.conn.Write(msg)
	return err
}

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

/* -------- 用于处理多数据库 --------*/

// SelectDB 选择数据库
func (c *Connection) SelectDB(idx int) {
	c.selectedDB = idx
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

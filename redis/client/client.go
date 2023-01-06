package client

import (
	"errors"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/lib/logger"
	"github.com/HildaM/GoKV/lib/sync/wait"
	"github.com/HildaM/GoKV/redis/parser"
	"github.com/HildaM/GoKV/redis/protocol"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

const (
	chanSize = 256
	maxWait  = 3 * time.Second
	retry    = 3 // 重试次数
)

type Client struct {
	conn net.Conn // 与服务端的tcp连接

	pendingReqs chan *request // 等待发送的请求
	waitingReqs chan *request // 等待服务器响应的请求
	ticker      *time.Ticker  // 用于触发心跳包的计时器
	addr        string

	status  int32           // client当前状态
	working *sync.WaitGroup // 用于优雅关闭。等待所有请求处理完成后才关闭连接
}

type request struct {
	id        uint64      // 请求id
	args      [][]byte    // 上行参数
	reply     redis.Reply // 收到的返回值
	heartbeat bool        // 标记是否是心跳请求
	waiting   *wait.Wait  // 调用协程发送请求后通过waitGroup等待请求异步处理完成
	err       error
}

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:        conn,
		addr:        addr,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start 启动所有异步协程
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go client.handleRead()
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

// Send 发送请求到后端服务器
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}
	// 封装请求
	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	// 标记请求
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()

	client.pendingReqs <- request // 将请求发送到处理队列中

	// 等待响应或者超时
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	// 处理返回的响应
	if request.err != nil {
		return protocol.MakeErrReply("request failed")
	}

	return request.reply
}

// handleWrite 写协程入口
func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// doRequest 处理每一个请求
func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	// 序列化数据
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()

	// 将数据发送到conn中
	var err error
	for i := 0; i < retry; i++ { // 重试次数
		_, err = client.conn.Write(bytes)
		// 如果没有错误，或者错误中不包含timeout、deadline等信息，则发送成功
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // 在reconnect方法中可以设置timeout错误
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}

	if err == nil {
		client.waitingReqs <- req // 发送至等待请求队列
	} else {
		req.err = err
		req.waiting.Done() // 请求出错，提前结束
	}
}

// handleRead 解析响应
func (client *Client) handleRead() {
	// 使用RESP协议解析器处理
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			// 1. 检查是否连接断开
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			// 2. 重连
			client.reconnect()
			return
		}

		// 完成请求，处理响应
		client.finishRequest(payload.Data)
	}
}

// finishedRequest 处理响应，完成请求
func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		// 异常处理 ---- 如果程序崩溃的话
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()

	request := <-client.waitingReqs // 获取等待队列中的request
	if request == nil {
		return
	}
	request.reply = reply // 将接收到的请求写入request中
	if request.waiting != nil {
		request.waiting.Done() // 正式结束请求
	}
}

// reconnect 链接断开后的重连
func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	_ = client.conn.Close() // 关闭当前连接。此处忽略可能的错误，因为此处的异常关闭连接不会影响程序接下来的运行

	var conn net.Conn
	for i := 0; i < retry; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			// 因为重连失败可能是网络问题，所以暂时睡眠一会，等待网络恢复
			// 在网络环境不好的时候，不要频繁重连，这样会加重网络拥塞
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}

	if conn == nil {
		// 重连失败
		client.Close()
		return
	}
	// 更新连接
	client.conn = conn

	// 将原本旧的连接关闭
	close(client.waitingReqs)
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done() // 提前结束
	}
	// 重新启动接收数据
	client.waitingReqs = make(chan *request, chanSize)
	// 重启处理响应的协程
	go client.handleRead()
}

// Close 优雅关闭
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	client.ticker.Stop()

	// 阻止新连接进入
	close(client.pendingReqs)
	// 等待所有连接处理完成
	client.working.Wait()

	// 清空连接
	client.conn.Close()
	close(client.waitingReqs)
}

// heartbeat 处理心跳
func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

// doHeartbeat PING-PONG请求
func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}

	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()

	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

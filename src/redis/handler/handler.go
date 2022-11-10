package handler

import (
	DBImpl "Godis/src/db"
	"Godis/src/interface/db"
	"Godis/src/lib/logger"
	"Godis/src/lib/sync/atomic"
	"Godis/src/redis/parser"
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"sync"
)

/*
	实现redis协议的tcp.Handler
*/

// 自定义异常情况
var (
	UnknownErrReplyBytes = []byte("-ERR unknow\r\n")
)

type Handler struct {
	activeConn sync.Map // 每个连接的映射表关系
	db         db.DB
	closing    atomic.AtomicBool // 关闭连接标志，阻止新连接继续访问
}

func MakeHandler() *Handler {
	return &Handler{
		db: DBImpl.MakeDB(),
	}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// 正在关闭，停止接收新请求
		conn.Close()
	}

	// 创建一个处理客户端
	client := &Client{conn: conn}
	// 读取请求
	reader := bufio.NewReader(conn)
	for {
		// 读取完整一行
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
			} else {
				logger.Warn(err)
			}

			client.Close()
			h.activeConn.Delete(client)
			return
		}

		if len(msg) == 0 {
			// 跳过空请求
			continue
		}

		// 如果当前没有正在发送的消息 ---- 新请求
		if !client.sending.Get() {
			// 数组消息：多条请求
			if msg[0] == '*' {
				// 将中间的有效消息截取，并转换为uint32格式（byte）
				expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					client.conn.Write(UnknownErrReplyBytes)
					continue
				}

				// TODO 弄明白为什么乘2
				// 猜测：将数据强行设置为偶数长度，方便后面判断是否出现错误
				expectedLine *= 2
				client.waittingReply.Add(1)
				client.sending.Set(true)
				client.expectedLineCount = uint32(expectedLine)
				client.sentLineCount = 0
				client.sentLines = make([][]byte, expectedLine)
			} else {
				// TODO: 其他处理协议
			}
		} else {
			// 当前存在发送的消息
			client.sentLines[client.sentLineCount] = msg[0 : len(msg)-2] // 排除后面的CRLF
			client.sentLineCount++

			// 如果发送完成
			if client.sentLineCount == client.expectedLineCount {
				client.sending.Set(false)
				// error
				if len(client.sentLines)%2 != 0 {
					client.conn.Write(UnknownErrReplyBytes)
					client.expectedLineCount = 0
					client.sentLineCount = 0
					client.sentLines = nil
					client.waittingReply.Done()
					continue
				}

				// send reply
				args := parser.Parse(client.sentLines)
				result := h.db.Exec(args)
				if result != nil {
					conn.Write(result.ToBytes())
				} else {
					conn.Write(UnknownErrReplyBytes)
				}

				// 发送完成后的清理工作
				client.expectedLineCount = 0
				client.sentLineCount = 0
				client.sentLines = nil
				client.waittingReply.Done()
			}
		}
	}
}

package server

import (
	DBImpl "Godis/database"
	"Godis/interface/database"
	"Godis/lib/logger"
	"Godis/lib/sync/atomic"
	"Godis/redis/connection"
	"Godis/redis/parser"
	"Godis/redis/protocol"
	"context"
	"io"
	"net"
	"strings"
	"sync"
)

/*
	实现redis协议的tcp.Handler
*/

// 自定义异常情况
var (
	unknownErrReplyBytes = []byte("-ERR unknow\r\n")
)

type Handler struct {
	activeConn sync.Map // 每个连接的映射表关系
	db         database.DB
	closing    atomic.AtomicBool // 关闭连接标志，阻止新连接继续访问
}

func MakeHandler() *Handler {
	return &Handler{
		db: DBImpl.MakeDB(),
	}
}

//func (h *Handler) Handler(ctx context.Context, conn net.Conn) {
//	if h.closing.Get() {
//		// 正在关闭，停止接收新请求
//		conn.Close()
//	}
//
//	// 创建一个处理客户端
//	client := &client2.Client{Conn: conn}
//	// 读取请求
//	reader := bufio.NewReader(conn)
//
//	var fixedLen int64 = 0
//	var err error
//	var msg []byte
//	for {
//		if fixedLen == 0 {
//			// 当前字符串不是命令字符串
//			msg, err = reader.ReadBytes('\n')
//		} else {
//			// 获取命令字符串
//			msg = make([]byte, fixedLen+2) // 加上CRLF两个字符
//			_, err = io.ReadFull(reader, msg)
//			fixedLen = 0
//		}
//
//		if err != nil {
//			if err == io.EOF || err == io.ErrUnexpectedEOF {
//				logger.Info("connection close")
//			} else {
//				logger.Warn(err)
//			}
//
//			client.Close()
//			h.activeConn.Delete(client)
//			return
//		}
//
//		if len(msg) == 0 {
//			// 跳过空请求
//			continue
//		}
//
//		// 如果当前没有正在发送的消息 ---- 新请求
//		if !client.sending.Get() {
//			// 数组消息：多条请求
//			if msg[0] == '*' {
//				// 将中间的有效消息截取，并转换为uint32格式（byte）
//				expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
//				if err != nil {
//					client.Conn.Write(UnknownErrReplyBytes)
//					continue
//				}
//
//				// TODO 弄明白为什么乘2
//				// 猜测：将数据强行设置为偶数长度，方便后面判断是否出现错误
//				expectedLine *= 2
//				client.waittingReply.Add(1)
//				client.sending.Set(true)
//				client.expectedLineCount = uint32(expectedLine)
//				client.sentLineCount = 0
//				client.sentLines = make([][]byte, expectedLine)
//			} else {
//				// TODO: 其他处理协议
//			}
//		} else {
//			line := msg[0 : len(msg)-2]
//			// 获取命令参数长度
//			if line[0] == '$' {
//				fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
//				if err != nil {
//					errReply := &protocol.ProtocolErrReply{Msg: err.Error()}
//					_, _ = client.Conn.Write(errReply.ToBytes())
//				}
//			}
//			client.sentLines[client.sentLineCount] = line
//			client.sentLineCount++
//
//			// 如果发送完成
//			if client.sentLineCount == client.expectedLineCount {
//				client.sending.Set(false)
//				// error
//				if len(client.sentLines)%2 != 0 {
//					_, _ = client.Conn.Write(UnknownErrReplyBytes)
//					client.expectedLineCount = 0
//					client.sentLineCount = 0
//					client.sentLines = nil
//					client.waittingReply.Done()
//					continue
//				}
//
//				// send protocol
//				args := parser.Parse(client.sentLines)
//				result := h.db.Exec(args)
//				if result != nil {
//					conn.Write(result.ToBytes())
//				} else {
//					conn.Write(UnknownErrReplyBytes)
//				}
//
//				// 发送完成后的清理工作
//				client.expectedLineCount = 0
//				client.sentLineCount = 0
//				client.sentLines = nil
//				client.waittingReply.Done()
//			}
//		}
//	}
//}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()

}

// Handle 接收并处理redis命令
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{}) // 使用空结构体更省内存

	ch := parser.ParseStream(conn) // 发送到协议解析器中处理
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 关闭连接
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			// protocol err（协议错误）
			// TODO
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}

		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("client shuting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		client.Close()
		return true
	})
	h.db.Close()
	return nil
}

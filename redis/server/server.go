package server

import (
	"context"
	"github.com/HildaM/GoKV/config"
	databaseImpl "github.com/HildaM/GoKV/database"
	"github.com/HildaM/GoKV/interface/database"
	"github.com/HildaM/GoKV/lib/logger"
	"github.com/HildaM/GoKV/lib/sync/atomic"
	"github.com/HildaM/GoKV/redis/connection"
	"github.com/HildaM/GoKV/redis/parser"
	"github.com/HildaM/GoKV/redis/protocol"
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
	var db database.DB
	if config.Properties.Self != "" ||
		len(config.Properties.Peers) != 0 {
		// TODO cluster 集群模式
	} else {
		db = databaseImpl.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	// TODO DB
	h.activeConn.Delete(client)
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
			// 读取到末尾
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 关闭连接
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}

			// protocol err（协议错误）
			errReply := protocol.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Error("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
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

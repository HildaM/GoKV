package tcp

import (
	"context"
	"net"
)

type HandleFunc func(ctx context.Context, conn net.Conn)

// Handler 是应用服务器的抽象
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

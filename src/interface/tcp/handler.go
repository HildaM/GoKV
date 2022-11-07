package tcp

import (
	"context"
	"net"
)

// Handler 是应用服务器的抽象
type Handler interface {
	Handler(ctx context.Context, conn net.Conn)
	Close() error
}

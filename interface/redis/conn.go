package redis

import "net"

/*
redis Connection 接口
*/
type Connection interface {
	RemoteAddr() net.Addr
}

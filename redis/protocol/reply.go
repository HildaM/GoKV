package protocol

import (
	"strconv"
)

/*
	redis响应reply
	RESP 通过第一个字符来表示格式：
		简单字符串：以"+" 开始， 如："+OK\r\n"
		错误：以"-" 开始，如："-ERR Invalid Synatx\r\n"
		整数：以":"开始，如：":1\r\n"
		字符串：以 $ 开始
		数组：以 * 开始
*/

var (
	nullBulkReplyBytes = []byte("$-1")
	CRLF               = "\r\n"
)

// 简单字符串
type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if len(r.Arg) == 0 {
		return nullBulkReplyBytes
	}
	// 返回bulkreply命令
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

// 数组
type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	if argLen == 0 {
		return nullBulkReplyBytes
	}

	// 确定命令长度
	res := "*" + strconv.Itoa(argLen) + CRLF
	for _, arg := range r.Args {
		res += "$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF
	}

	return []byte(res)
}

// 简单状态
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + "\r\n")
}

// 错误消息
type ErrReply struct {
	Status string
}

func MakeErrReply(status string) *ErrReply {
	return &ErrReply{
		Status: status,
	}
}

func (r *ErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + "\r\n")
}

// 数字消息
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

package parser

import (
	"Godis/interface/redis"
	"Godis/lib/logger"
	"Godis/redis/protocol"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
)

// Payload 存储redis.Reply或者error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream 从io中读取数据，并使用channel放回payload
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()

	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		// len <= 2是不合法的，至少有3个以上的字符，毕竟CRLF就已经len=2了
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			protocolError(ch, line)
			continue
		}

		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		// 根据命令的不同前缀处理
		switch line[0] {
		case '+':
			ch <- &Payload{
				Data: protocol.MakeStatusReply(string(line[1:])),
			}
		case '-':
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, line)
				continue
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		case '$':
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			// 非标识字符串
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}

	}
}

// parseArray 处理多条命令
func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 1. 获取命令长度
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, header)
		return err
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}

	// 2. 获取接下来的命令
	bodys := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		// 1. 获取命令长度
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return nil
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			// $(num)CRLF：最低长度为4
			protocolError(ch, line)
			return nil
		}

		// 2. 获取真实命令，并将其放入bodys中
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, line)
			return nil
		} else if strLen == 0 {
			bodys = append(bodys, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err = io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			bodys = append(bodys, body)
		}
	}

	// 返回最终结果
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(bodys),
	}
	return nil
}

// parseBulkString 处理字符串数据
func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 1. 获取命令长度
	cmdLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || cmdLen < -1 { // 正常情况：$(num), header[1:]的长度至少为1
		protocolError(ch, header)
		return err
	} else if cmdLen == -1 { // 空命令
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}

	// 2. 获取命令本体
	body := make([]byte, cmdLen+2) // 加上CRLF
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}

	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]), // 减去CRLF
	}
	return nil
}

// protocolError 协议异常情况处理封装方法
func protocolError(ch chan<- *Payload, line []byte) {
	err := errors.New(fmt.Sprintf("Protocol error: %s", string(line[1:])))
	ch <- &Payload{Err: err}
}

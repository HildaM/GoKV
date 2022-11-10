package parser

import (
	"Godis/interface/redis"
	"Godis/lib/logger"
	"Godis/redis/protocol"
	"bufio"
	"bytes"
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

// TODO
func parseArray(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	return nil
}

func parseBulkString(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	return nil
}

func protocolError(ch chan<- *Payload, line []byte) {
	return
}

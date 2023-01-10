package client

import (
	"bytes"
	"github.com/HildaM/GoKV/lib/logger"
	"github.com/HildaM/GoKV/redis/protocol"
	"testing"
	"time"
)

/*
	内含敏感信息，不要上传！！！
*/

func TestClient(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	// 连接必须存在，可以用远程服务器的redis连接
	client, err := MakeClient("120.25.153.110:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	// 连接远程服务器，先进行认证
	auth := client.Send([][]byte{
		[]byte("auth"),
		[]byte("austin"),
	})
	if res, ok := auth.(*protocol.StatusReply); ok {
		if res.Status != "OK" {
			t.Error("auth failed")
		}
	}

	// 验证各种命令
	result := client.Send([][]byte{
		[]byte("SET"),
		[]byte("a"),
		[]byte("a"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "OK" {
			t.Error("`set` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if bulkRet, ok := result.(*protocol.BulkReply); ok {
		if string(bulkRet.Arg) != "a" {
			t.Error("`get` failed, result: " + string(bulkRet.Arg))
		}
	}

}

/*
请使用自己的redis服务器进行测试
auth [your password]
IP:6379
*/
func TestReconnect(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	// 连接必须存在，可以用远程服务器的redis连接
	client, err := MakeClient("IP:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	_ = client.conn.Close()
	time.Sleep(time.Second) // wait for reconnecting
	success := false

	// 连接远程服务器，先进行认证
	_ = client.Send([][]byte{
		[]byte("auth"),
		[]byte("[your password]"),
	})

	for i := 0; i < 3; i++ {
		result := client.Send([][]byte{
			[]byte("PING"),
		})
		t.Logf("\n%d: result return: %s", i, string(result.ToBytes()))
		if bytes.Equal(result.ToBytes(), []byte("+PONG\r\n")) {
			success = true
			break
		}
	}
	if !success {
		t.Error("reconnect error")
	}
}

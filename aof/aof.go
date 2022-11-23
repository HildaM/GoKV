package aof

import (
	"Godis/config"
	"Godis/interface/database"
	"Godis/lib/logger"
	"Godis/lib/utils"
	"Godis/redis/connection"
	"Godis/redis/parser"
	"Godis/redis/protocol"
	"io"
	"os"
	"strconv"
	"sync"
)

// CmdLine -> [][]byte的别名，用于表示一行命令
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

// aof的信息载体
type payLoad struct {
	cmdLine CmdLine
	dbIndex int
}

// Handler 处理aof逻辑的处理器，从channel管道中接收aof消息，然后进行aof写入
type Handler struct {
	db         database.EmbedDB
	tmpDBMaker func() database.EmbedDB
	currentDB  int

	// aof处理相关
	aofChan     chan *payLoad // 接收aof消息的管道
	aofFile     *os.File
	aofFileName string
	aofFinished chan struct{} // 与主协程通信的管道，通知主协程aof写入完成
	pausingAof  sync.RWMutex  // 终止当前aof写入，以便开始或结束aof重写
}

// NewAOFHandler
func NewAOFHandler(db database.EmbedDB, tmpDBMaker func() database.EmbedDB) (*Handler, error) {
	handler := &Handler{}
	handler.aofFileName = config.Properties.AppendFilename
	handler.db = db
	handler.tmpDBMaker = tmpDBMaker

	handler.LoadAOF(0) // 加载aof文件
	aofFile, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payLoad, aofQueueSize)
	handler.aofFinished = make(chan struct{})

	// 另起一个子协程，处理aof
	go func() {
		handler.handleAof()
	}()

	return handler, nil
}

// AddAof 向处理aof的协程发送消息，将命令加入到aof文件中
func (handler *Handler) AddAof(dbIndex int, cmdline CmdLine) {
	// 先判断是否可以加入
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payLoad{
			cmdLine: cmdline,
			dbIndex: dbIndex,
		}
	}
}

// LoadAOF 加载aof文件
func (handler *Handler) LoadAOF(maxBytes int) {
	// 加载aof文件时，需要暂时将aofChan关闭，避免重复写入
	aofChan := handler.aofChan
	handler.aofChan = nil
	// 完成aof加载后，需要将原本被删除的aofChan管道重新初始化，避免之后aof无法正常使用
	defer func(aofChan chan *payLoad) {
		handler.aofChan = aofChan
	}(aofChan)

	// 打开aof文件
	file, err := os.Open(handler.aofFileName)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}

	ch := parser.ParseStream(reader)
	fakeConn := &connection.FakeConn{} // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		// 将解析出来的命令逐一执行
		ret := handler.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", ret.ToBytes())
		}
	}
}

// handleAof listen aof channel and write into file
func (handler *Handler) handleAof() {
	handler.currentDB = 0 // 设定当前默认数据库
	for p := range handler.aofChan {
		// 共享锁
		handler.pausingAof.RLock()

		// 如果数据库不同，则需要先切换数据库
		if p.dbIndex != handler.currentDB {
			// select db
			data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			// 切换数据库
			handler.currentDB = p.dbIndex
		}

		data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}

		// 解锁
		handler.pausingAof.RUnlock()
	}

	// aof完成写入，发送消息
	handler.aofFinished <- struct{}{}
}

// Close 优雅关闭
func (handler *Handler) Close() {
	if handler.aofFile != nil {
		close(handler.aofChan)         // 不再接收新的aof写入
		<-handler.aofFinished          // 等待所有aof写入完成
		err := handler.aofFile.Close() // 正式关闭
		if err != nil {
			logger.Warn(err)
		}
	}
}

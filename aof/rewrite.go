package aof

import (
	"Godis/config"
	"Godis/interface/database"
	"Godis/lib/logger"
	"Godis/lib/utils"
	"Godis/redis/protocol"
	"io"
	"os"
	"strconv"
	"time"
)

/*
	AOF 重写
*/

// RewriteCtx 重写上下文
type RewriteCtx struct {
	tmpFile  *os.File // 存储aof的临时文件
	fileSize int64
	dbIndex  int
}

func (handler *Handler) newRewriteHandler() *Handler {
	h := &Handler{}
	h.aofFileName = handler.aofFileName
	h.db = handler.tmpDBMaker()
	return h
}

// Rewrite 重写方法的上层接口
func (handler *Handler) Rewrite() error {
	ctx, err := handler.StartRewrite()
	if err != nil {
		return nil
	}
	err = handler.DoRewrite(ctx)
	if err != nil {
		return nil
	}
	handler.FinishRewrite(ctx)
	return nil
}

// StartRewrite 为重写的开始做准备
func (handler *Handler) StartRewrite() (*RewriteCtx, error) {
	// 1. 暂时停止重写操作，关闭aofChan。此时新数据会堆积在aofChan上
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()

	// 2. 调用fsync是缓冲区的数据落盘，防止aof数据不完整
	err := handler.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// 3. 获取当前aof文件消息，以备之后判断哪些数据是重写过程中产生的新数据
	fileInfo, err := os.Stat(handler.aofFileName)
	if err != nil {
		logger.Warn("get file stat failed")
		// return nil, err	// 暂时略过错误，继续执行
	}
	fileSize := fileInfo.Size()

	// 4. 创建aof文件的临时副本
	// tmpFile, err := os.CreateTemp("", "*.aof") // ERROR: 直接创建temp文件在C盘中，而不是项目路径内
	tmpFile, err := os.Create("tmpAofFile.aof")
	if err != nil {
		logger.Warn("create temp file failed")
		return nil, err
	}

	return &RewriteCtx{
		tmpFile:  tmpFile,
		fileSize: fileSize,
		dbIndex:  0,
	}, nil
}

// DoRewrite 执行aof重写
func (handler *Handler) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile

	// 1. 加载旧的aof数据
	tmpAof := handler.newRewriteHandler()
	tmpAof.LoadAOF(int(ctx.fileSize)) // TODO 打断点调试！

	// 2. 执行重写操作
	for i := 0; i < config.Properties.Databases; i++ {
		// select db：遍历每一个db，备份数据
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}

		// 定义写入aof的匿名函数
		writeDataToAof := func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			// 遍历到的每一个数据，都对其执行func方法中的操作 ————> golang的函数式编程
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd = MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		}

		handler.db.ForEach(i, writeDataToAof)
	}

	return nil
}

// FinishRewrite 结束aof重写
func (handler *Handler) FinishRewrite(ctx *RewriteCtx) {
	// 1. 暂停aof写入
	handler.pausingAof.Lock()
	defer handler.pausingAof.Unlock()

	// 2. 操作旧的aof文件，将文件指针指向重写开始前的位置
	oldAof, err := os.Open(handler.aofFileName) // 只读模式启动
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return
	}
	//defer func() {
	//	// 不要忘记关闭资源！！！
	//	_ = oldAof.Close()
	//}()

	// 文件指针指向重写开启前的位置
	_, err = oldAof.Seek(ctx.fileSize, 0)
	if err != nil {
		logger.Error("oldAof seek failed: " + err.Error())
		return
	}

	// 3. 在tmpAof文件末尾插入一条Select语句，确保tmpAof末尾的数据库选择，与重写开始后新数据写入的选择一致，保证新的aof文件连贯性
	tmpAof := ctx.tmpFile
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIndex))).ToBytes()
	_, err = tmpAof.Write(data)
	if err != nil {
		logger.Error("tmpAof rewrite failed: " + err.Error())
		return
	}

	// 4. 将新数据拷贝到tmpAof文件中
	_, err = io.Copy(tmpAof, oldAof)
	if err != nil {
		logger.Error("copy aof to tmpAof failed: " + err.Error())
		return
	}

	// 5. 替换掉原有的aof文件
	_ = handler.aofFile.Close() // 先关闭原有文件，再做更新处理
	_ = oldAof.Close()
	_ = tmpAof.Close()
	_ = os.Rename(tmpAof.Name(), handler.aofFileName)

	// 6. 重新打开aofFile，并更新到handler中
	aofFile, err := os.OpenFile(handler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		// 此处如果打不开，那么程序必须崩溃，因为aof文件已经被修改了，出现错误无法补救
		panic(err)
	}
	handler.aofFile = aofFile // 更新

	// 7. 数据库同步。在aof文件中追加一条select，确保与当前currentDB一致
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(handler.currentDB))).ToBytes()
	_, err = handler.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}

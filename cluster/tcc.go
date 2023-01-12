package cluster

import (
	"fmt"
	"github.com/HildaM/GoKV/database"
	"github.com/HildaM/GoKV/interface/redis"
	"github.com/HildaM/GoKV/lib/logger"
	"github.com/HildaM/GoKV/lib/timewheel"
	"github.com/HildaM/GoKV/lib/utils"
	"github.com/HildaM/GoKV/redis/protocol"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxLockTime       = 3 * time.Second
	waitBeforeCleanTx = 2 * maxLockTime

	createdStatus    = 0
	preparedStatus   = 1
	committedStatus  = 2
	rolledBackStatus = 3
)

// Transaction stores state and data for a try-commit-catch distributed transaction
type Transaction struct {
	id      string   // 事务id，由协调者使用雪花算法生成
	cmdLine [][]byte // 事务命令

	cluster *Cluster         // 集群
	conn    redis.Connection // 数据库连接
	dbIndex int

	writeKeys  []string  // 事务待操作的key
	readKeys   []string  // 事务待读取的key
	keysLocked bool      // keys上锁标志
	undoLog    []CmdLine // 回滚命令

	status int8
	mu     *sync.Mutex
}

// NewTransaction creates a try-commit-catch distributed transaction
func NewTransaction(cluster *Cluster, c redis.Connection, id string, cmdLine [][]byte) *Transaction {
	return &Transaction{
		id:      id,
		cmdLine: cmdLine,
		cluster: cluster,
		conn:    c,
		dbIndex: c.GetDBIndex(),
		status:  createdStatus,
		mu:      new(sync.Mutex),
	}
}

var prepareFuncMap = make(map[string]CmdFunc)

// 将prepare函数注册
func registerPrepareFunc(cmdName string, fn CmdFunc) {
	prepareFuncMap[strings.ToLower(cmdName)] = fn
}

// execPrepare tcc事务 —— try阶段
// cmdLine: txID cmdName [k1, v1, k2, v2, k3, v3.....]
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'prepare' command")
	}

	txID := string(cmdLine[1])
	cmdName := strings.ToLower(string(cmdLine[2]))
	tx := NewTransaction(cluster, c, txID, cmdLine[2:]) // 事务处理器
	cluster.transactions.Put(txID, tx)                  // 将txID对应的事务放入列表中，等待commit阶段的执行

	// try
	err := tx.prepare()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// 查看该命令是否有前置准备函数，有则执行
	prepareFunc, ok := prepareFuncMap[cmdName]
	if ok {
		return prepareFunc(cluster, c, cmdLine[2:])
	}

	return &protocol.OkReply{}
}

// prepare 实际的准备函数
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(tx.cmdLine)
	// 锁住所有将要写入的keys
	tx.lockKeys()

	// 生成undo回滚命令
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)

	// 标记当前事务处理器的状态为“准备阶段”
	tx.status = preparedStatus

	// 处理准备阶段超时情况：监控准备阶段的维持时间，如果超时，那么将判定为“失败”，随机启动回滚操作
	taskKey := genTaskKey(tx.id)
	// 启动异步协程监控
	timewheel.Delay(maxLockTime, taskKey, func() {
		// 如果超过maxLockTime（最大键锁定时间），则强制解锁，避免阻塞时间过长
		if tx.status == preparedStatus {
			logger.Info("Transaction ERR: prepare timeout, abort transaction: " + tx.id)
			tx.mu.Lock()
			defer tx.mu.Unlock()

			err := tx.rollbackWithLock() // 回滚命令
			if err != nil {
				logger.Warn(err.Error())
			}
		}
	})

	return nil
}

func (tx *Transaction) rollbackWithLock() error {
	curStatus := tx.status

	if tx.status != curStatus {
		return fmt.Errorf("tx %s status changed", tx.id) // 并发操作可能会导致变量中途被修改
	}
	// 已经被回滚了
	if tx.status == rolledBackStatus {
		return nil
	}

	// 回滚操作
	tx.lockKeys()
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithLock(tx.conn, cmdLine)
	}
	tx.unLockKeys()

	tx.status = rolledBackStatus
	return nil
}

func genTaskKey(txID string) string {
	return "tx:" + txID
}

// lockKeys 独享锁，锁住将要修改的keys
func (tx *Transaction) lockKeys() {
	if !tx.keysLocked {
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = true
	}
}

// unLockKeys 解除独享锁
func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// requestCommit 将所有节点的事务提交
func requestCommit(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) ([]redis.Reply, protocol.ErrorReply) {
	var errReply protocol.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)
	respList := make([]redis.Reply, 0, len(groupMap))

	for node := range groupMap {
		var resp redis.Reply
		if node == cluster.self {
			resp = execCommit(cluster, c, utils.ToCmdLine2("commit", txIDStr))
		} else {
			resp = cluster.relay(node, c, utils.ToCmdLine2("commit", txIDStr))
		}

		if protocol.IsErrorReply(resp) {
			errReply = resp.(protocol.ErrorReply)
			break
		}

		respList = append(respList, resp)
	}

	if errReply != nil {
		requestRollback(cluster, c, txID, groupMap)
		return nil, errReply
	}

	return respList, nil
}

// execCommit 实际执行commit的代码
func execCommit(cluster *Cluster, c redis.Connection, cmdLine [][]byte) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}

	// 从待执行的事务列表中取出制定的事务
	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID) // 在execPrepare阶段放入transactions列表的事务处理器tx
	if !ok {
		return protocol.MakeIntReply(0)
	}

	tx, _ := raw.(*Transaction)

	// 提交事务
	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := cluster.db.ExecWithLock(c, tx.cmdLine)
	if protocol.IsErrorReply(result) {
		// 命令执行失败，混滚代码
		err := tx.rollbackWithLock()
		return protocol.MakeErrReply(fmt.Sprintf("err occurs when rollback: %v, origin err: %s", err, result))
	}

	// 本地事务提交后的处理
	tx.unLockKeys() // 解除keys的锁
	tx.status = committedStatus
	// 清除所有已完成的事务。但不要立即清除，以防回滚操作
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})

	return result
}

// requestRollback 协调者请求所有参与节点回滚
func requestRollback(cluster *Cluster, c redis.Connection, txID int64, groupMap map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 64)
	for node := range groupMap {
		if node == cluster.self {
			execRollback(cluster, c, utils.ToCmdLine2("rollback", txIDStr))
		} else {
			cluster.relay(node, c, utils.ToCmdLine2("rollback", txIDStr))
		}
	}
}

// execRollback 回滚所有本地事务
func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}

	txID := string(cmdLine[1])
	raw, ok := cluster.transactions.Get(txID)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	tx := raw.(*Transaction)

	// 执行回滚
	tx.mu.Lock()
	defer tx.mu.Unlock()

	err := tx.rollbackWithLock()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// 延时清除所有的事务。延时的目的在于等待其他节点完成
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.transactions.Remove(tx.id)
	})

	return protocol.MakeIntReply(1)
}

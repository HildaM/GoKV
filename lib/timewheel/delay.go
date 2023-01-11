package timewheel

import (
	"container/list"
	"time"
)

type location struct {
	slot  int
	etask *list.Element // 任务详情
}

// task 任务封装类
type task struct {
	delay  time.Duration // 任务延时执行时间点
	circle int           // 任务循环次数
	key    string
	job    func() // 具体任务
}

// TimeWheel 定时任务
type TimeWheel struct {
	interval time.Duration // 时间
	ticker   *time.Ticker  // 定时器
	slots    []*list.List  // 任务队列

	timer             map[string]*location
	currentPos        int         // 当前位置
	slotNum           int         // 任务数量
	addTackChannel    chan task   // 接收任务队列
	removeTaskChannel chan string // 已完成任务队列
	stopChannel       chan bool   // 取消任务队列
}

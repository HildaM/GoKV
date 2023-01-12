package timewheel

import (
	"container/list"
	"github.com/HildaM/GoKV/lib/logger"
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

// New 构造函数
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		logger.Warn("WARN the number of timewheel New function argument is wrong!")
		return nil
	}

	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTackChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()

	return tw
}

// initSlots 初始化list链表
func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start 启动定时器
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

func (tw *TimeWheel) start() {
	// 监听各队列情况
	for {
		select {
		case <-tw.ticker.C: // 定时任务处理
			tw.tickHandler()
		case task := <-tw.addTackChannel: // 新增任务
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel: // 移除任务
			tw.removeTask(key)
		case <-tw.stopChannel: // 关闭定时队列进程
			tw.ticker.Stop()
			return
		}
	}
}

// Stop 关闭延迟队列
// 将暂停消息发送至stopChannel队列即可
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// tickHandler 定时器响应
func (tw *TimeWheel) tickHandler() {
	task := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0 // 一个循环
	} else {
		tw.currentPos++
	}

	go tw.scanAndRunTask(task) // 执行任务
}

// scanAndRunTask 遍历任务队列，并执行任务
func (tw *TimeWheel) scanAndRunTask(t *list.List) {
	for e := t.Front(); e != nil; {
		task := e.Value.(*task)
		if task.circle > 0 { // 需要等到circle个循环后，才执行当前的任务
			task.circle--
			e = e.Next()
			continue
		}

		// 异步执行任务，提高并发效率
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()

			job := task.job
			job() // 执行任务
		}()

		// 移除当前任务
		next := e.Next()
		t.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}

		e = next
	}
}

// AddJob 添加任务的上层接口
func (tw *TimeWheel) AddJob(deley time.Duration, key string, job func()) {
	if deley < 0 {
		return
	}
	tw.addTackChannel <- task{delay: deley, key: key, job: job}
}

// addTask 新增定时任务
func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}

	if task.key != "" {
		_, ok := tw.timer[task.key] // 如果已经存在相同任务
		if ok {
			tw.removeTask(task.key)
		}
	}

	tw.timer[task.key] = loc
}

func (tw *TimeWheel) getPositionAndCircle(delay time.Duration) (pos int, circle int) {
	delaySeconds := int(delay.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum
	return
}

// RemoveJob 移除任务上层接口
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

// removeTask 移除任务
func (tw *TimeWheel) removeTask(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}

	l := tw.slots[pos.slot] // 获取所在的任务队列
	l.Remove(pos.etask)
	delete(tw.timer, key)
}

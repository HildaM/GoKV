package timewheel

import (
	"time"
)

var tw = New(time.Second, 3600)

func init() {
	tw.Start()
}

// Delay 将任务延迟指定时间执行
func Delay(duration time.Duration, key string, job func()) {
	tw.AddJob(duration, key, job)
}

// At 提交的任务必须在截至时间（at）内完成
// Sub returns the duration t-u  ---> func (t Time) Sub(u Time) Duration
func At(at time.Time, key string, job func()) {
	tw.AddJob(at.Sub(time.Now()), key, job)
}

// Cancel 取消任务
func Cancel(key string) {
	tw.RemoveJob(key)
}

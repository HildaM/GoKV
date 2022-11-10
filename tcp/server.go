package tcp

import (
	"Godis/interface/tcp"
	"Godis/lib/logger"
	"Godis/lib/sync/atomic"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServe(cfg *Config, handler tcp.Handler) {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		logger.Fatal(fmt.Sprintf("listen err: %v", err))
	}

	// 监听中断信号并通过 closeChan 通知服务器关闭
	var closing atomic.AtomicBool
	sigCh := make(chan os.Signal, 1)
	// 使用sigCh监听操作系统的信号
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 为每一个连接开启一个协程，监听系统信号
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logger.Info("shuting down...")
			closing.Set(true)
			listener.Close()
		}
	}()

	// 监听端口
	logger.Info(fmt.Sprintf("bind: %s, starting listening...", cfg.Address))
	// 作关闭操作。需要先关闭listener，再关闭handler
	// defer 后进先出执行
	defer handler.Close()
	defer listener.Close()

	// 创建一个带有取消函数chancel的上下文context
	ctx, _ := context.WithCancel(context.Background())
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				return // 直接退出
			}
			logger.Error(fmt.Sprintf("accept err: %v", err))
			continue
		}

		logger.Info("accept link")
		go handler.Handle(ctx, conn)
	}
}

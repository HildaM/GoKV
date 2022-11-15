package tcp

import (
	"Godis/interface/tcp"
	"Godis/lib/logger"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ListenAndServerWithSignal 绑定IP+端口。响应系统中断停止
func ListenAndServerWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{}) // 用于协程同步
	sigCh := make(chan os.Signal)    // 接收系统信号
	// 使用sigCh监听操作系统的信号
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 为每一个连接开启一个协程，监听系统信号
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		log.Fatal(err)
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe 监听端口，同时监听系统中断信号
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan chan struct{}) {
	// 监听信号
	go func() {
		<-closeChan
		logger.Info("shutting down....")
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 监听端口
	defer func() {
		// 遇到未明错误而中断连接
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var wait sync.WaitGroup // 实现多个协程并发执行
	for {
		// 接收请求
		conn, err := listener.Accept()
		if err != nil {
			logger.Fatal(err)
			break
		}

		// handle
		logger.Info("accept link")
		wait.Add(1)
		go func() {
			defer wait.Done()
			handler.Handle(ctx, conn)
		}()
	}
	wait.Wait() // 等待所有协程返回
}

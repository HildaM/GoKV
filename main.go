package main

import (
	"Godis/lib/logger"
	"Godis/redis/server"
	"Godis/tcp"
	"time"
)

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	tcp.ListenAndServe(&tcp.Config{
		Address:    ":6399",
		MaxConnect: 16,
		Timeout:    2 * time.Second,
	}, server.MakeHandler())
}

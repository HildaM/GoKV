package main

import (
	"Godis/src/lib/logger"
	"Godis/src/server"
	"time"
)

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	server.ListenAndServe(&server.Config{
		Address:    ":6399",
		MaxConnect: 16,
		Timeout:    2 * time.Second,
	}, server.MakeEchoHandler())
}

package main

import (
	"Godis/src/server"
	"time"
)

func main() {

	server.ListenAndServe(&server.Config{
		Address:    ":6399",
		MaxConnect: 16,
		Timeout:    2 * time.Second,
	}, server.MakeEchoHandler())
}

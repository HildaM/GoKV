package server

import "time"

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

//func ListenAndServer(cfg *Config, handler tcp.Handler) {
//
//}

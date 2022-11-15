package config

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	src := "bind 0.0.0.0\n" +
		"port 6399\n" +
		"maxclients 128\n" +
		"appendonly no\n" +
		"appendfilename appendonly.aof\n" +
		"peers a,b"
	p := parse(strings.NewReader(src))

	if p == nil {
		t.Error("cannot get result")
		return
	}
	if p.Bind != "0.0.0.0" {
		t.Error("string parse failed")
	}
	if p.Port != 6399 {
		t.Error("int parse failed")
	}
	if p.MaxClients != 128 {
		t.Error("maxcient parse failed")
	}
	if p.AppendOnly {
		t.Errorf("bool parse failed: %v", p.AppendOnly)
	}
	if p.AppendFilename != "appendonly.aof" {
		t.Error("appendfilename parse error")
	}
	if len(p.Peers) != 2 || p.Peers[0] != "a" || p.Peers[1] != "b" {
		t.Error("list parse failed")
	}
}

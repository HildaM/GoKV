package config

import (
	"bufio"
	"github.com/HildaM/GoKV/lib/logger"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	Bind              string `cfg:"bind"`                // 绑定IP
	Port              int    `cfg:"port"`                // 绑定端口
	AppendOnly        bool   `cfg:"appendonly"`          // 是否开启AOF持久化模式
	AppendFilename    string `cfg:"appendfilename"`      // AOF文件名设置
	MaxClients        int    `cfg:"maxclients"`          // 最大接收客户端数量
	RequirePass       string `cfg:"requirepass"`         // redis认证密码
	Databases         int    `cfg:"databases"`           // 可用数据库数量
	RDBFilename       string `cfg:"dbfilename"`          // RDB文件名
	MasterAuth        string `cfg:"masterauth"`          // master主服务器认证密码
	SlaveAnnouncePort int    `cfg:"slave-announce-port"` // 备份服务器端口
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`   // 备份服务器IP地址
	ReplTimeout       int    `cfg:"repl-timeout"`        // 主从复制超时时间

	Peers []string `cfg:"peers"` // 备份服务器存储
	Self  string   `cfg:"self"`
}

// 全局配置类
var Properties *ServerProperties

func init() {
	// default config
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
	}
}

// parse 解析redis.conf文件
/*
redis.conf 文件格式：
	bind 0.0.0.0
	port 6399
	maxclients 128
*/
func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src) // 带有缓存的读取流
	for scanner.Scan() {
		line := scanner.Text() // 获取一行

		// 跳过空行和注释行
		if len(line) == 0 || strings.TrimLeft(line, " ")[0] == '#' {
			continue
		}

		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 {
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value // cfg限制，要求转换为小写
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// 使用反射，填充空结构体config
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)

		// 检测并获取tag
		key, ok := field.Tag.Lookup("cfg")
		if !ok && strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}

		val, ok := rawMap[strings.ToLower(key)]
		if ok {
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(val)
			case reflect.Int:
				intVal, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					logger.Fatal(err)
				}
				fieldVal.SetInt(intVal)
			case reflect.Bool:
				boolVal := "yes" == val
				fieldVal.SetBool(boolVal)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(val, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}

	return config
}

// SetupConfig
func SetupConfig(configFileName string) {
	file, err := os.Open(configFileName)
	defer file.Close()
	if err != nil {
		logger.Fatal(err)
	}
	Properties = parse(file)
}

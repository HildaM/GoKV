package utils

// ToCmdLine 将字符串数组转换为[][]byte命令行
func ToCmdLine(args ...string) [][]byte {
	bytes := make([][]byte, len(args))
	for i, v := range args {
		bytes[i] = []byte(v)
	}
	return bytes
}

func ToCmdLine2(cmdName string, args ...string) [][]byte {
	bytes := make([][]byte, len(args)+1)
	bytes[0] = []byte(cmdName)
	for i, v := range args {
		bytes[i+1] = []byte(v)
	}
	return bytes
}

func ToCmdLine3(cmdName string, args ...[]byte) [][]byte {
	bytes := make([][]byte, len(args)+1)
	bytes[0] = []byte(cmdName)
	for i, v := range args {
		bytes[i+1] = v
	}
	return bytes
}

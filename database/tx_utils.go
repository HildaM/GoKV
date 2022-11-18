package database

/*
	事务处理工具类
*/

func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func readFirstKey(args [][]byte) ([]string, []string) {
	// assert len(args) > 0
	key := string(args[0])
	return nil, []string{key}
}

func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	//key := string(args[0])
	// return rollbackGivenKeys(db, key)
	return nil
}

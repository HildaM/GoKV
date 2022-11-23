package dict

// 只提供最基本的kv存储的dict结构
type SimpleDict struct {
	m map[string]interface{}
}

// MakeSimple makes a new map
func MakeSimple() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

// Get
func (dict *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m[key]
	return val, ok
}

func (dict *SimpleDict) Len() int {
	if dict.m == nil {
		panic("dict is nil")
	}
	return len(dict.m)
}

func (dict *SimpleDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m[key]
	dict.m[key] = val
	if existed {
		return 0
	}
	return 1
}

func (dict *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	if _, ok := dict.m[key]; !ok {
		dict.m[key] = val
		return 1
	}
	return 0
}

func (dict *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	if _, ok := dict.m[key]; ok {
		dict.m[key] = val
		return 1
	}
	return 0
}

func (dict *SimpleDict) Remove(key string) (result int) {
	if _, existed := dict.m[key]; existed {
		delete(dict.m, key)
		return 1
	}
	return 0
}

func (dict *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range dict.m {
		if !consumer(k, v) {
			break
		}
	}
}

func (dict *SimpleDict) Keys() []string {
	keys := make([]string, len(dict.m))
	for k := range dict.m {
		keys = append(keys, k)
	}
	return keys
}

func (dict *SimpleDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range dict.m {
			result[i] = k
			break
		}
	}
	return result
}

func (dict *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > len(dict.m) {
		size = len(dict.m)
	}

	res := make([]string, size)
	i := 0
	for k := range dict.m {
		if i == size {
			break
		}
		res = append(res, k)
		i++
	}
	return res
}

func (dict *SimpleDict) Clear() {
	dict.m = make(map[string]interface{})
}

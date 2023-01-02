package sortedset

import (
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 16
)

// Element 对外的元素抽象
type Element struct {
	Member string
	Score  float64
}

// Node
type node struct {
	Element           // 元素的名称和 score
	backward *node    // 后向指针
	level    []*Level // 前向指针, level[0] 为最下层
}

// Level 节点中每一层的抽象
type Level struct {
	forward *node // 指向同层中的下一个节点
	span    int64 // 到 forward 跳过的节点数
}

// skiplist 跳表定义
type skiplist struct {
	header *node
	tail   *node
	length int64
	level  int16
}

func makeNode(level int16, score float64, member string) *node {
	n := &node{
		Element: Element{
			Member: member,
			Score:  score,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

// getByRank 跳表查找元素
func (skiplist *skiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := skiplist.header
	// 从当前层向前搜索
	// 若当前层的下一个节点已经超过目标 (i+n.level[level].span > rank)，则结束当前层搜索进入下一层
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span   // 向前跳跃
			n = n.level[level].forward // 下一个节点
		}
		// 如果内层for循环退出，说明当前层已经溢出，此时level就会自增到下一层，从下一层开始继续寻找

		if i == rank {
			return n
		}
	}
	return nil
}

// getFirstInScoreRange 获取指定范围内的第一个元素
func (skiplist *skiplist) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	// 预处理：如果[min, max]不在zset范围内，则直接退出
	if !skiplist.hasInRange(min, max) {
		return nil
	}

	n := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		// 自顶向下搜索
		// 如果forward已经进入范围，但是此时不能确定forward的值是范围内的第一个，需要继续搜索直到最底层
		for n.level[level].forward != nil && !min.less(n.level[level].forward.Score) { // score >= min
			n = n.level[level].forward
		}
	}

	// 当循环退出时，此时已经来到了最底层，此时的forward是范围内的第一个数
	n = n.level[0].forward
	if !max.greater(n.Score) { // 判断当前值是否越界
		return nil
	}

	return n
}

// getLastInScoreRange 获取范围内最后一个数值
func (skiplist *skiplist) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *node {
	if !skiplist.hasInRange(min, max) {
		return nil
	}

	n := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		// 只有当 max <= score才跳到下一层搜索
		for n.level[level].forward != nil && max.greater(n.level[level].forward.Score) {
			n = n.level[level].forward
		}
	}

	if !min.less(n.Score) { // 保证当前值不小于min
		return nil
	}
	return n
}

// hasInRange 判断当前范围是否可以覆盖zset数值范围
func (skiplist *skiplist) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
	// 异常判断
	if (min.Value > max.Value) || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
		return false
	}

	// min > tail
	n := skiplist.tail
	if n == nil || !min.less(n.Score) {
		return false
	}

	// max < head
	n = skiplist.header.level[0].forward
	if n == nil || !max.greater(n.Score) {
		return false
	}

	return true // min <= tail < head <= max
}

// insert 插入数据
func (skiplist *skiplist) insert(member string, score float64) *node {
	// 寻找新节点的先驱节点，它们的 forward 将指向新节点
	// 因为每层都有一个 forward 指针, 所以每层都会对应一个先驱节点
	// 找到这些先驱节点并保存在 update 数组中
	update := make([]*node, maxLevel)
	rank := make([]int64, maxLevel) // 保存各层先驱节点的排名，用以计算span

	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- { // 自顶向下寻找
		if i == skiplist.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1] // store rank that is crossed to reach the insert position
		}

		if node.level[i] != nil {
			// 遍历搜索
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				// 相同score，但是member不同，也需要继续搜索
				rank[i] += node.level[i].span
				node = node.level[i].forward // 向前遍历
			}
		}

		update[i] = node // 记录当前层的先驱节点
	}

	level := randomLevel() // 随机决定新节点有多少层
	// 如果新level大于原本的层数，则需要创建新层
	if level > skiplist.level {
		for i := skiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = skiplist.header
			update[i].level[i].span = skiplist.length
		}
		skiplist.level = level
	}

	// 创建新节点并插入跳表  ---> 自底向上插入
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		// 新节点的forward节点是先驱节点的下一个节点
		node.level[i].forward = update[i].level[i].forward
		// 先驱节点的下一个节点是新节点
		update[i].level[i].forward = node

		// 计算先驱节点和新节点的span
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 新节点可能不会包含所有层。对于没有新节点的层，先驱节点的span会加一（因为后面插入了新节点导致）
	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	// 更新后向节点
	if update[0] == skiplist.header {
		update[0].backward = nil
	} else {
		node.backward = update[0] // 指向node的先驱节点
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node // 指向自己
	} else {
		skiplist.tail = node // 若插入点正是末尾，则更新tail
	}

	skiplist.length++
	return node
}

// randomLevel 随机生成索引层数
func randomLevel_new() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k)) + 1
}

func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}

	if level < maxLevel {
		return level
	}
	return maxLevel
}

// 删除节点 ---- 可能一次删除多个节点
// 删除逻辑大致上与插入逻辑类似
func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0
	update := make([]*node, maxLevel)
	removed = make([]*Element, 0)

	// 自顶向下寻找目标的先驱节点
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			// 确保当前寻找的值比start小，这样确保最后找到的数据是start开头的
			i += node.level[level].span
			node = node.level[level].forward
		}

		update[level] = node
	}

	i++
	node = node.level[0].forward // node是范围内的第一个节点

	// 删除范围内的所有节点
	for node != nil && i < stop {
		next := node.level[0].forward
		removeElement := node.Element
		removed = append(removed, &removeElement)

		// 删除node
		skiplist.removeNode(node, update)

		node = next
		i++
	}

	return removed
}

// removeNode 删除单个节点
func (skiplist *skiplist) removeNode(node *node, update []*node) {
	for i := int16(0); i < skiplist.level; i++ {
		// 修改update中先驱节点的指针，同时更新span的值
		if update[i].level[i].forward == node {
			update[i].level[i].forward = node.level[i].forward
			update[i].level[i].span += node.level[i].span - 1
		} else {
			// forward不是node节点
			update[i].level[i].span--
		}
	}

	// 更新修改目标节点的后继节点的backward指针
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		skiplist.tail = node.backward
	}

	// 删除空白的层
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}

	// 节点总数减一
	skiplist.length--
}

package sortedset

import "strconv"

// SortedSet 对外提供的操作接口，封装skiplist的方法对外服务
type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

// Add 增加元素。如果已经存在member，则返回false
func (sortedSet *SortedSet) Add(member string, score float64) bool {
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{ // 更新member的值
		Member: member,
		Score:  score,
	}
	// 如果存在且score不相等，则先移除旧的，再放入新的
	if ok {
		if score != element.Score {
			sortedSet.skiplist.remove(member, element.Score)
			sortedSet.skiplist.insert(member, score)
		}
		return false
	}

	sortedSet.skiplist.insert(member, score)
	return true
}

// Get 获取指定元素
func (sortedSet *SortedSet) Get(member string) (*Element, bool) {
	element, ok := sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, ok
}

// Remove 删除指定的member
func (sortedSet *SortedSet) Remove(member string) bool {
	v, ok := sortedSet.dict[member]
	if !ok {
		return false
	}

	sortedSet.skiplist.remove(member, v.Score)
	delete(sortedSet.dict, member)
	return true
}

// Len
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

// GetRank 获取指定member的排序，默认按照升序排列，从0开始
func (sortedSet *SortedSet) GetRank(member string, desc bool) int64 {
	v, ok := sortedSet.dict[member]
	if !ok {
		return -1
	}

	rank := sortedSet.skiplist.getRank(member, v.Score)
	if desc {
		rank = sortedSet.skiplist.length - rank
	} else {
		rank--
	}

	return rank
}

// ForEach 遍历整个跳表
func (sortedSet *SortedSet) ForEach(start, end int64, desc bool, consumer func(element *Element) bool) {
	size := int64(sortedSet.Len())
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	} else if end < start || end > size {
		panic("illegal end " + strconv.FormatInt(end, 10))
	}

	// 1. 寻找开头的节点
	var node *node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(size - start)) // 寻找倒序开头（即end元素）
		}
	} else {
		node = sortedSet.skiplist.header
		if start > 0 {
			node = sortedSet.skiplist.getByRank(int64(start + 1))
		}
	}

	// 2. 开始遍历
	sliceSize := int(end - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward // 向前遍历
		} else {
			node = node.level[0].forward // 向后遍历
		}
	}
}

// Range 遍历[start, end)范围内的元素，同时按照指定的遍历方式
func (sortedSet *SortedSet) Range(start, end int64, desc bool) []*Element {
	sliceSize := int(end - start)
	slice := make([]*Element, sliceSize)

	i := 0
	sortedSet.ForEach(start, end, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})

	return slice
}

// Count 记录在指定范围内的元素数量
func (sortedSet *SortedSet) Count(min *ScoreBorder, max *ScoreBorder) int64 {
	var count int64 = 0

	// 按升序遍历
	sortedSet.ForEach(0, sortedSet.Len(), false, func(element *Element) bool {
		lessThanMin := min.less(element.Score)
		if !lessThanMin {
			return true // 比min小，不在范围内，继续遍历
		}
		greaterThanMax := max.greater(element.Score)
		if !greaterThanMax {
			return false // 比max大，越界，直接退出
		}

		count++
		return true
	})

	return count
}

// ForEachByScore 遍历在范围内的元素
func (sortedSet *SortedSet) ForEachByScore(min, max *ScoreBorder, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	// 1. 寻找遍历起点
	var node *node
	if desc {
		node = sortedSet.skiplist.getLastInScoreRange(min, max)
	} else {
		node = sortedSet.skiplist.getFirstInScoreRange(min, max)
	}

	// 2. 如果给出了offset，则先移动offset距离
	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	// 3. 获取数据
	// 如果limit<0，则将获取所有数据
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}

		greaterThanMin := min.less(node.Element.Score)
		lessThanMax := max.greater(node.Element.Score)
		if !greaterThanMin || !lessThanMax {
			// 不大于Min：说明没有进入范围。
			// 大于Max：说明越界
			break
		}
	}
}

// RangeByScore 获取范围内的元素
func (sortedSet *SortedSet) RangeByScore(min, max *ScoreBorder, offset, limit int64, desc bool) []*Element {
	slice := make([]*Element, 0)
	if limit == 0 || offset < 0 {
		return slice
	}

	sortedSet.ForEachByScore(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})

	return slice
}

// RemoveByScore 删除指定范围内的元素
func (sortedSet *SortedSet) RemoveByScore(min, max *ScoreBorder) int64 {
	removed := sortedSet.skiplist.RemoveRangeByScore(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

// RemoveByRank 删除指定rank的元素
// [start, end) 从0开始计数
func (sortedSet *SortedSet) RemoveByRank(start, end int64) int64 {
	removed := sortedSet.skiplist.RemoveRangeByRank(start+1, end+1)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

// PopMin 弹出count个最小值
func (sortedSet *SortedSet) PopMin(count int) []*Element {
	// 寻找全局最小值
	first := sortedSet.skiplist.getFirstInScoreRange(negativeInfBorder, positiveInfBorder)
	startBorder := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}

	removed := sortedSet.skiplist.RemoveRangeByScore(startBorder, positiveInfBorder, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return removed
}

package sortedset

import (
	"math"
	"testing"
)

func TestRandomLevel(t *testing.T) {
	oldRandom := make(map[int16]int)
	for i := 0; i < 100000; i++ {
		level := randomLevel()
		oldRandom[level]++
	}
	for i := 0; i <= maxLevel; i++ {
		t.Logf("oldRandom: i = %d, count = %d", i, oldRandom[int16(i)])
	}
	t.Logf("期望：%d", mean(oldRandom))
	t.Logf("方差：%d", variance(oldRandom))
	t.Logf("标准差：%f", std(oldRandom))

	t.Log(" ************************** ")

	newRandom := make(map[int16]int)
	for i := 0; i < 100000; i++ {
		level := randomLevel_new()
		newRandom[level]++
	}
	for i := 0; i <= maxLevel; i++ {
		t.Logf("newRandom: i = %d, count = %d", i, newRandom[int16(i)])
	}
	t.Logf("期望：%d", mean(newRandom))
	t.Logf("方差：%d", variance(newRandom))
	t.Logf("标准差：%f", std(newRandom))

	t.Log(" ************************** ")

	carlRandom := make(map[int16]int)
	for i := 0; i < 100000; i++ {
		level := carl_RandomLevel()
		carlRandom[level]++
	}
	for i := 0; i <= maxLevel; i++ {
		t.Logf("carlRandom: i = %d, count = %d", i, carlRandom[int16(i)])
	}
	t.Logf("期望：%d", mean(carlRandom))
	t.Logf("方差：%d", variance(carlRandom))
	t.Logf("标准差：%f", std(carlRandom))

}

// 求方差、平均差、期望
func mean(v map[int16]int) int {
	var res int = 0
	var n int = len(v)
	for i := 0; i < n; i++ {
		res += v[int16(i)]
	}
	return res / n
}

func variance(v map[int16]int) int {
	var res int = 0
	var m = mean(v)
	var n int = len(v)
	for i := 0; i < n; i++ {
		res += (v[int16(i)] - m) * (v[int16(i)] - m)
	}
	return res / (n - 1)
}
func std(v map[int16]int) float64 {
	return math.Sqrt(float64(variance(v)))
}

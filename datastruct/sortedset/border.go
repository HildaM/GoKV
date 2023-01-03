package sortedset

/*
ScoreBorder 是一个封装类。用以在ZRangeByScore命令中表示min、max等范围
支持的范围：
	1. int float（包括正负数）
	2. infinity: +inf -inf（正无穷、负无穷
*/

const (
	negativeInf int8 = -1
	positiveInf int8 = 1
)

// ScoreBorder represents range of a float value, including: <, <=, >, >=, +inf, -inf
type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

// if max.greater(score) then the score is within the upper border
// do not use min.greater()
func (border *ScoreBorder) greater(value float64) bool {
	if border.Inf == negativeInf { // 负无穷
		return false
	} else if border.Inf == positiveInf { // 正无穷
		return true
	}
	if border.Exclude { // 溢出
		return border.Value > value
	}

	return border.Value >= value
}

// less 判断当前值是否比border更小
func (border *ScoreBorder) less(value float64) bool {
	if border.Inf == negativeInf {
		return true
	} else if border.Inf == positiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

var positiveInfBorder = &ScoreBorder{
	Inf: positiveInf,
}

var negativeInfBorder = &ScoreBorder{
	Inf: negativeInf,
}

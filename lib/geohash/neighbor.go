package geohash

import (
	"math"
)

/*
	计算附近的地理位置
*/

const (
	dr          = math.Pi / 180.0
	earthRadius = 6372797.560856
	mercatorMax = 20037726.37 // pi * earthRadius
	mercatorMin = -20037726.37
)

// estimatePrecisionByRadius
func estimatePrecisionByRadius(radiusMeters float64, latitude float64) uint {
	// 极限精度
	if radiusMeters == 0 {
		return defaultBitSize - 1
	}

	var precision uint = 1
	for radiusMeters < mercatorMax {
		radiusMeters *= 2
		precision++
	}

	/* Make sure range is included in most of the base cases. */
	precision -= 2
	if latitude > 66 || latitude < -66 {
		precision--
		if latitude > 80 || latitude < -80 {
			precision--
		}
	}
	if precision < 1 {
		precision = 1
	}
	if precision > 32 {
		precision = 32
	}
	return precision*2 - 1
}

// toRange 计算geohash对应的矩形上下界
func toRange(scope []byte, precision uint) [2]uint64 {
	lower := ToInt(scope)
	radius := uint64(1 << (64 - precision)) // 将radius转换为二进制，方便与geohash相加
	upper := lower + radius
	return [2]uint64{lower, upper}
}

// GetNeighbours 返回给定经纬度的附近点的坐标
func GetNeighbours(latitude, longitude, radiusMeters float64) [][2]uint64 {
	// 获取合适精度
	precision := estimatePrecisionByRadius(radiusMeters, latitude)

	center, box := encode0(latitude, longitude, precision) // 将给定经纬度设置为中心点
	height := box[0][1] - box[0][0]                        // 经度长度
	width := box[1][1] - box[1][0]                         // 纬度长度
	centerLongitude := (box[0][1] + box[0][0]) / 2
	centerLatitude := (box[1][1] + box[1][0]) / 2
	maxLatitude := ensureValidLatitude(centerLatitude + height)
	minLatitude := ensureValidLatitude(centerLatitude - height)
	maxLongitude := ensureValidLongitude(centerLongitude + width)
	minLongitude := ensureValidLongitude(centerLongitude - width)

	// 构造九宫格
	var result [10][2]uint64
	// 左上角
	leftUpper, _ := encode0(maxLatitude, minLongitude, precision)
	result[1] = toRange(leftUpper, precision)

	// 正上方
	upper, _ := encode0(maxLongitude, centerLatitude, precision)
	result[2] = toRange(upper, precision)

	// 右上角
	rightUpper, _ := encode0(maxLongitude, maxLatitude, precision)
	result[3] = toRange(rightUpper, precision)

	// 左边
	left, _ := encode0(centerLatitude, minLongitude, precision)
	result[4] = toRange(left, precision)

	// 中间
	result[5] = toRange(center, precision)

	// 右边
	right, _ := encode0(centerLatitude, maxLongitude, precision)
	result[6] = toRange(right, precision)

	// 左下角
	leftDown, _ := encode0(minLatitude, minLongitude, precision)
	result[7] = toRange(leftDown, precision)

	// 下面
	down, _ := encode0(minLatitude, centerLongitude, precision)
	result[8] = toRange(down, precision)

	// 右下角
	rightDown, _ := encode0(minLatitude, centerLongitude, precision)
	result[9] = toRange(rightDown, precision)

	return result[1:]
}

// 地球是圆的，经度成环，所以可以用360修正不正确的经度
func ensureValidLongitude(longitude float64) float64 {
	if longitude > 180 {
		return longitude - 360
	}
	if longitude < -180 {
		return longitude + 360
	}
	return longitude
}

// 纬度不成环，必须在[-90, 90]范围内
func ensureValidLatitude(latitude float64) float64 {
	if latitude > 90 {
		return 90
	}
	if latitude < -90 {
		return -90
	}
	return latitude
}

// Distance 计算两点之间的最短距离
func Distance(latitude1, longitude1, latitude2, longitude2 float64) float64 {
	radLat1 := degRad(latitude1)
	radLat2 := degRad(latitude2)
	a := radLat1 - radLat2
	b := degRad(longitude1) - degRad(longitude2)

	return 2 * earthRadius * math.Asin(math.Sqrt(math.Pow(math.Sin(a/2), 2)+
		math.Cos(radLat1)*math.Cos(radLat2)*math.Pow(math.Sin(b/2), 2)))
}

func degRad(ang float64) float64 {
	return ang * dr
}

func radDeg(ang float64) float64 {
	return ang / dr
}

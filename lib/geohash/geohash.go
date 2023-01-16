package geohash

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
)

// 自定义bit处理
var bits = []uint8{128, 64, 32, 16, 8, 4, 2, 1} // 大端序

// 32 bits for latitude, another 32 bits for longitude
const defaultBitSize = 64

// GeoHash的映射表与标准Base32映射表不同，需要进行文本格式转化
var enc = base32.NewEncoding("0123456789bcdefghjkmnpqrstuvwxyz").WithPadding(base32.NoPadding)

// encode0
/*
	算法类似与二分搜索
	box是使用算法划分后的二维空间
*/
func encode0(latitude, longitude float64, bitSize uint) ([]byte, [2][2]float64) {
	// GeoHash矩阵
	box := [2][2]float64{
		{-180, 180}, // 经度轴 longitude
		{-90, 90},   // 纬度轴 latitude
	}

	pos := [2]float64{longitude, latitude}
	hash := &bytes.Buffer{}
	bit := 0
	var precision uint = 0
	code := uint8(0)

	// 循环执行到精度足够
	for precision < bitSize {
		// 轮流处理经纬度。此循环可以将GeoHash推广到N维空间
		for direction, val := range pos { // direction：表示经度纬度替换的参数
			mid := (box[direction][0] + box[direction][1]) / 2 // 计算两个经度（维度）之间的中点
			if val < mid {
				box[direction][1] = mid // 更新右端点
				// 编码为0，不需要操作
			} else {
				box[direction][0] = mid // 更新左端点
				code |= bits[bit]
				// 通过位或操作写入1，比如要在字节的第3位写入1应该 code |= 32
			}

			// bit进位
			bit++
			if bit == 8 { // 计算完一个字节的编码，将其写入buffer
				hash.WriteByte(code)
				bit = 0
				code = 0
			}

			// 精度增加
			precision++
			if precision == bitSize {
				break
			}
		}
	}

	// 将剩余的字符写入
	if code > 0 {
		hash.WriteByte(code)
	}

	// 返回结果
	return hash.Bytes(), box
}

// Encode 上层解析接口
func Encode(latitude, longitude float64) uint64 {
	buf, _ := encode0(latitude, longitude, defaultBitSize)
	return binary.BigEndian.Uint64(buf) // 将数据解析为uint64格式
}

// 解码算法
// 根据hash的二进制信息，逆向还原二分编码的数据
func decode0(hash []byte) [][]float64 {
	box := [][]float64{
		{-180, 180},
		{-90, 90},
	}

	direction := 0
	for i := 0; i < len(hash); i++ {
		code := hash[i]
		for j := 0; j < len(bits); j++ {
			mid := (box[direction][0] + box[direction][1]) / 2
			mask := bits[j] // 使用掩码取出指定位
			if mask&code > 0 {
				// 对应上面 val >= mid 情况 ---> 经(纬)度大于mid
				box[direction][0] = mid
			} else {
				// 对应上面 val < mid 情况 ---> 经(纬)度小于mid
				box[direction][1] = mid
			}

			direction = (direction + 1) % 2 // 经纬度轮流处理
		}
	}

	return box
}

// Decode
func Decode(code uint64) (float64, float64) {
	buf := make([]byte, 8)
	// 转换位大端序存储
	binary.BigEndian.PutUint64(buf, code) // 每8个bit作为一个byte处理，进入buf队列

	// 取方格子的中点作为最终推算的地理位置
	box := decode0(buf)
	longitude := float64(box[0][0]+box[0][1]) / 2
	latitude := float64(box[1][0]+box[1][1]) / 2

	return latitude, longitude
}

/** 数据转换函数 */

// ToString 将bytes格式的geohash转换位base32的字符串
func ToString(buf []byte) string {
	return enc.EncodeToString(buf)
}

// ToInt 将bytes格式的geohash转换位uint64格式的
// 转换为大端序存储
func ToInt(buf []byte) uint64 {
	if len(buf) < 8 {
		// 不足8位
		buf2 := make([]byte, 8)
		copy(buf2, buf2) // 将其补全
		return binary.BigEndian.Uint64(buf2)
	}
	return binary.BigEndian.Uint64(buf)
}

// FromInt 将uint64格式的geohash转换位bytes
func FromInt(code uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, code)
	return buf
}

package geohash

import (
	"math"
	"testing"
)

func TestMyEncode(t *testing.T) {
	geohash, box := encode0(179.99519348, 89.99794006, 7)
	t.Logf("geohash: %v, box: %v", ToString(geohash), box)
}

//func TestToRange(t *testing.T) {
//	neighbor := []byte{0x00, 0x00, 0x00, 0x00, 0xE0, 0x00, 0x00, 0x00}
//	geoRange := toRange(neighbor, 36)
//	expectedLower := ToInt([]byte{0x00, 0x00, 0x00, 0x00, 0xE0, 0x00, 0x00, 0x00})
//	expectedUpper := ToInt([]byte{0x00, 0x00, 0x00, 0x00, 0xF0, 0x00, 0x00, 0x00})
//	if expectedLower != geoRange[0] {
//		t.Error("incorrect lower")
//	}
//	if expectedUpper != geoRange[1] {
//		t.Error("incorrect upper")
//	}
//}

func TestEncode(t *testing.T) {
	lat0 := 48.669
	lng0 := -4.32913
	hash := Encode(lat0, lng0)
	t.Logf("hash: %v", hash)

	str := ToString(FromInt(hash))
	if str != "gbsuv7zt7zntw" {
		t.Error("encode error")
	}

	lat, lng := Decode(hash)
	if math.Abs(lat-lat0) > 1e-6 || math.Abs(lng-lng0) > 1e-6 {
		// 确保误差在1e-6内
		t.Error("decode error")
	}
}

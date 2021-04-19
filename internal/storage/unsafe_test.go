package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsafe(t *testing.T) {
	buf := []byte{16}
	int8Res := UnsafeReadInt8(buf, 0)
	assert.Equal(t, int8Res, int8(16))

	buf = []byte{16, 16}
	int16Res := UnsafeReadInt16(buf, 0)
	assert.Equal(t, int16Res, int16(4112))

	buf = []byte{16, 16, 16, 16}
	int32Res := UnsafeReadInt32(buf, 0)
	assert.Equal(t, int32Res, int32(269488144))

	buf = []byte{16, 16, 16, 16, 16, 16, 16, 16}
	int64Res := UnsafeReadInt64(buf, 0)
	assert.Equal(t, int64Res, int64(1157442765409226768))

	buf = []byte{16, 16, 16, 16}
	float32Res := UnsafeReadFloat32(buf, 0)
	assert.Equal(t, float32Res, float32(2.8411367e-29))

	buf = []byte{16, 16, 16, 16, 16, 16, 16, 16}
	float64Res := UnsafeReadFloat64(buf, 0)
	assert.Equal(t, float64Res, float64(2.586563270614692e-231))
}

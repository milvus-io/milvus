package storage

import "unsafe"

func UnsafeReadInt8(buf []byte, idx int) int8 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int8)(ptr))
}

func UnsafeReadInt16(buf []byte, idx int) int16 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int16)(ptr))
}

func UnsafeReadInt32(buf []byte, idx int) int32 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int32)(ptr))
}

func UnsafeReadInt64(buf []byte, idx int) int64 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*int64)(ptr))
}

func UnsafeReadFloat32(buf []byte, idx int) float32 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*float32)(ptr))
}

func UnsafeReadFloat64(buf []byte, idx int) float64 {
	ptr := unsafe.Pointer(&(buf[idx]))
	return *((*float64)(ptr))
}

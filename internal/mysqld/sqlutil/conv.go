package sqlutil

import "strconv"

func Float32ToString(f float32) string {
	return strconv.FormatFloat(float64(f), 'f', -1, 32)
}

func Float64ToString(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

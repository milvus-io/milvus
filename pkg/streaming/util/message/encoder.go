package message

import "strconv"

const base = 36

// EncodeInt64 encodes int64 to string.
func EncodeInt64(value int64) string {
	return strconv.FormatInt(value, base)
}

// EncodeUint64 encodes uint64 to string.
func EncodeUint64(value uint64) string {
	return strconv.FormatUint(value, base)
}

// DecodeUint64 decodes string to uint64.
func DecodeUint64(value string) (uint64, error) {
	return strconv.ParseUint(value, base, 64)
}

// DecodeInt64 decodes string to int64.
func DecodeInt64(value string) (int64, error) {
	return strconv.ParseInt(value, base, 64)
}

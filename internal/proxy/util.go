package proxy

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"unsafe"
)

func Hash32_Bytes(b []byte) (uint32, error) {
	h := murmur3.New32()
	if _, err := h.Write(b); err != nil {
		return 0, err
	}
	return h.Sum32() & 0x7fffffff, nil
}

func Hash32_Uint64(v uint64) (uint32, error) {
	b := make([]byte, unsafe.Sizeof(v))
	binary.LittleEndian.PutUint64(b, v)
	return Hash32_Bytes(b)
}


func Hash32_Int64(v int64) (uint32, error) {
	return Hash32_Uint64(uint64(v))
}
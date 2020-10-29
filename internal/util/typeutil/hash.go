package typeutil

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"unsafe"
)

func Hash32Bytes(b []byte) (uint32, error) {
	h := murmur3.New32()
	if _, err := h.Write(b); err != nil {
		return 0, err
	}
	return h.Sum32() & 0x7fffffff, nil
}

func Hash32Uint64(v uint64) (uint32, error) {
	b := make([]byte, unsafe.Sizeof(v))
	binary.LittleEndian.PutUint64(b, v)
	return Hash32Bytes(b)
}


func Hash32Int64(v int64) (uint32, error) {
	return Hash32Uint64(uint64(v))
}

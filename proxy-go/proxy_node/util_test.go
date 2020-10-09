package proxy_node

import (
	"testing"
	"unsafe"
)

func TestUint64(t *testing.T) {
	var i int64 = -1
	var u uint64 = uint64(i)
	t.Log(i)
	t.Log(u)
}

func TestHash32_Uint64(t *testing.T) {
	var u uint64 = 0x12
	h, err := Hash32_Uint64(u)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(h)

	b := make([]byte, unsafe.Sizeof(u))
	b[0] = 0x12
	h2, err := Hash32_Bytes(b)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(h2)
	if h != h2 {
		t.Fatalf("Hash function failed")
	}
}

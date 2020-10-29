package typeutil
import (
	"github.com/stretchr/testify/assert"
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
	h, err := Hash32Uint64(u)
	assert.Nil(t, err)

	t.Log(h)

	b := make([]byte, unsafe.Sizeof(u))
	b[0] = 0x12
	h2, err := Hash32Bytes(b)
	assert.Nil(t, err)

	t.Log(h2)
	assert.Equal(t, h, h2)
}


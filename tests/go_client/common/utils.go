package common

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/x448/float16"
	"go.uber.org/zap"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func GenRandomString(prefix string, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	str := fmt.Sprintf("%s_%s", prefix, string(b))
	return str
}

// GenLongString gen invalid long string
func GenLongString(n int) string {
	var builder strings.Builder
	longString := "a"
	for i := 0; i < n; i++ {
		builder.WriteString(longString)
	}
	return builder.String()
}

func GenValidNames() []string {
	return []string{
		"a",
		"_",
		"_name",
		"_123",
		"name_",
		"_coll_123_",
	}
}

func GenInvalidNames() []string {
	invalidNames := []string{
		"",
		"   ",
		"12-s",
		"(mn)",
		"中文",
		"%$#",
		"1",
		"[10]",
		"a  b",
		DefaultDynamicFieldName,
		GenLongString(MaxCollectionNameLen + 1),
	}
	return invalidNames
}

func GenFloatVector(dim int) []float32 {
	vector := make([]float32, 0, dim)
	for j := 0; j < int(dim); j++ {
		vector = append(vector, rand.Float32())
	}
	return vector
}

func GenFloat16Vector(dim int) []byte {
	ret := make([]byte, dim*2)
	for i := 0; i < int(dim); i++ {
		v := float16.Fromfloat32(rand.Float32()).Bits()
		binary.LittleEndian.PutUint16(ret[i*2:], v)
	}
	return ret
}

func GenBFloat16Vector(dim int) []byte {
	ret16 := make([]uint16, 0, dim)
	for i := 0; i < int(dim); i++ {
		f := rand.Float32()
		bits := math.Float32bits(f)
		bits >>= 16
		bits &= 0x7FFF
		ret16 = append(ret16, uint16(bits))
	}
	ret := make([]byte, len(ret16)*2)
	for i, value := range ret16 {
		binary.LittleEndian.PutUint16(ret[i*2:], value)
	}
	return ret
}

func GenBinaryVector(dim int) []byte {
	vector := make([]byte, dim/8)
	rand.Read(vector)
	return vector
}

func GenSparseVector(maxLen int) entity.SparseEmbedding {
	length := 1 + rand.Intn(1+maxLen)
	positions := make([]uint32, length)
	values := make([]float32, length)
	for i := 0; i < length; i++ {
		positions[i] = uint32(2*i + 1)
		values[i] = rand.Float32()
	}
	vector, err := entity.NewSliceSparseEmbedding(positions, values)
	if err != nil {
		log.Fatal("Generate vector failed %s", zap.Error(err))
	}
	return vector
}

package distance

import (
	"math"

	"golang.org/x/sys/cpu"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/distance/asm"
)

func init() {
	if cpu.X86.HasAVX2 {
		log.Info("Hook avx for go simd distance computation")
		IPImpl = asm.IP
		L2Impl = asm.L2
		CosineImpl = func(a []float32, b []float32) float32 {
			return asm.IP(a, b) / float32(math.Sqrt(float64(asm.IP(a, a))*float64((asm.IP(b, b)))))
		}
	}
}

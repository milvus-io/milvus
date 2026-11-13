package distance

import (
	"context"
	"math"

	"golang.org/x/sys/cpu"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/distance/asm"
)

func init() {
	if cpu.X86.HasAVX2 {
		mlog.Info(context.TODO(), "Hook avx for go simd distance computation")
		IPImpl = asm.IP
		L2Impl = asm.L2
		CosineImpl = func(a []float32, b []float32) float32 {
			normA := asm.IP(a, a)
			normB := asm.IP(b, b)
			// zero-norm vectors: cosine is undefined; return 0 (not NaN 0/0) to
			// match the pure implementation, knowhere's search semantics and the
			// fused int8/fp16/bf16 kernels
			if normA == 0 || normB == 0 {
				return 0
			}
			return asm.IP(a, b) / float32(math.Sqrt(float64(normA)*float64(normB)))
		}
	}
}

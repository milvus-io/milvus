package distance

import (
	"context"

	"golang.org/x/sys/cpu"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/distance/asm"
)

func init() {
	if cpu.X86.HasAVX2 {
		mlog.Info(context.TODO(), "Hook avx for go simd distance computation")
		IPImpl = asm.IP
		L2Impl = asm.L2
		// The AVX2 cosine hook accumulated squared norms in float32, which
		// underflows to 0 for vectors with norm below ~1e-19 and returned
		// NaN instead of staying scale-invariant (milvus-io/milvus#53903).
		// Delegate to the float64-accumulating pure implementation.
		CosineImpl = CosineImplPure
	}
}

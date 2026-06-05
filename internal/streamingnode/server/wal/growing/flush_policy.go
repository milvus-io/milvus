package growing

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type flushPolicy interface {
	ShouldFlush(buffer writeOnlyInsertBuffer, now uint64) bool
}

type writeOnlyFlushPolicy struct {
	maxRows  uint64
	maxBytes uint64
	maxAge   time.Duration
}

func newDefaultWriteOnlyFlushPolicy() flushPolicy {
	return newWriteOnlyFlushPolicy(
		0,
		uint64(paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()),
		paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second),
	)
}

func newWriteOnlyFlushPolicy(maxRows, maxBytes uint64, maxAge time.Duration) flushPolicy {
	return writeOnlyFlushPolicy{
		maxRows:  maxRows,
		maxBytes: maxBytes,
		maxAge:   maxAge,
	}
}

func (p writeOnlyFlushPolicy) ShouldFlush(buffer writeOnlyInsertBuffer, now uint64) bool {
	if len(buffer.entries) == 0 {
		return false
	}
	if p.maxRows > 0 && buffer.rows >= p.maxRows {
		return true
	}
	if p.maxBytes > 0 && buffer.binarySize >= p.maxBytes {
		return true
	}
	if p.maxAge > 0 {
		start := tsoutil.PhysicalTime(buffer.fromTimeTick)
		current := tsoutil.PhysicalTime(now)
		return current.Sub(start) > p.maxAge
	}
	return false
}

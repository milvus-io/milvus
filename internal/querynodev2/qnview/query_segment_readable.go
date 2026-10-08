package qnview

import (
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// SegmentReadView borrows native resources pinned by a query handle.
// LoadInfo is an immutable applied snapshot, never the pending preparation target.
type SegmentReadView struct {
	Collection   *segcore.CCollection
	Segment      segcore.CSegment
	LoadInfo     *querypb.SegmentLoadInfo
	DatabaseName string
}
type ReadableSealedSegment interface{ ReadView() SegmentReadView }

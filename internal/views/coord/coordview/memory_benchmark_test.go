package coordview

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var memoryViewSink *viewpb.QueryViewOfShard

func BenchmarkMemoryStateSnapshot(b *testing.B) {
	for _, count := range []int{10, 10000} {
		b.Run(fmt.Sprintf("segments%d", count), func(b *testing.B) {
			ids := make([]int64, count)
			manifests := make([]int64, count)
			for i := range ids {
				ids[i] = int64(i)
				manifests[i] = 1
			}
			sm := &CoordQueryViewStateMachine{view: &viewpb.QueryViewOfShard{
				Meta:          &viewpb.QueryViewMeta{CollectionId: 1, ReplicaId: 10, Vchannel: "by-dev-rootcoord-dml_0_1v0", Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1}},
				QueryNode:     []*viewpb.QueryViewOfQueryNode{{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 1, SegmentIds: ids, SegmentManifestVersions: manifests}}}},
				StreamingNode: &viewpb.QueryViewOfStreamingNode{},
			}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				memoryViewSink = sm.viewWithState(qviews.QueryViewStateDown)
			}
		})
	}
}

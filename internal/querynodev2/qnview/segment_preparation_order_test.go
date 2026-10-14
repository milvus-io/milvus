package qnview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPublishedSchemaRecoveryPreemptsUnmaterializedTarget(t *testing.T) {
	for _, initial := range []bool{true, false} {
		t.Run(map[bool]string{true: "initial_load", false: "reopen"}[initial], func(t *testing.T) {
			scheduler := &fakeNodeScheduler{}
			stream := &fakeSegmentLoadInfoStream{}
			manager := newTestViewScopedPhysicalSegmentManager(t, scheduler, stream)
			view := &viewpb.QueryViewOfQueryNode{NodeId: 2, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
			ready := make(chan int32, 4)
			acquire := func(version int64, schema int32) {
				meta := buildHandlerTestMeta(version)
				meta.SchemaVersion, meta.LoadInfoVersion = schema, uint64(schema)
				manager.Acquire(AcquirePhysicalSegments{
					Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), Meta: meta, View: view,
					Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID, schemaVersion: int64(schema)},
					OnLoaded:   func([]TransformSegment) { ready <- schema }, OnUnrecoverable: func() { t.Error("recoverable published schema was discarded") },
				})
			}
			snapshot := SegmentLoadInfoSnapshot{
				CollectionID: testCollectionID, SegmentID: 1000,
				Revision: SegmentLoadInfoRevision{Revision: 10}, LoadInfo: &querypb.SegmentLoadInfo{CollectionID: testCollectionID, SegmentID: 1000, PartitionID: 10},
			}
			if initial {
				acquire(3, 2)
				require.NoError(t, stream.Emit(snapshot))
			} else {
				acquire(1, 0)
				require.NoError(t, stream.Emit(snapshot))
				scheduler.Tasks()[0].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
				require.EqualValues(t, 0, <-ready)
				acquire(3, 2)
			}
			// Recovery has an older DataView order, but must not wait for the
			// target's derived column to materialize.
			acquire(2, 1)
			if initial {
				scheduler.Tasks()[0].OnUnrecoverable(merr.WrapErrServiceNotReadyMsg("target function output is absent"))
				require.Len(t, scheduler.Tasks(), 2)
				require.EqualValues(t, 1, scheduler.Tasks()[1].Collection.SchemaVersion())
				scheduler.Tasks()[1].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
			} else {
				require.True(t, scheduler.Updates()[0].OnRetry())
				require.Len(t, scheduler.Updates(), 2)
				require.EqualValues(t, 1, scheduler.Updates()[1].Collection.SchemaVersion())
				scheduler.Updates()[1].OnUpdated(snapshot.Revision)
			}
			require.EqualValues(t, 1, <-ready)
			require.Empty(t, ready, "target must remain unready")
			updates := scheduler.Updates()
			require.EqualValues(t, 2, updates[len(updates)-1].Collection.SchemaVersion())
			updates[len(updates)-1].OnUpdated(snapshot.Revision)
			require.EqualValues(t, 2, <-ready)
			require.Empty(t, ready, "each schema receives one prepared read snapshot")
			require.Len(t, stream.subscriptions, 1)
		})
	}
}

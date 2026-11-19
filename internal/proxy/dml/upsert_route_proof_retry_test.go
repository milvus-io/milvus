package dml

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The documented route/proof refusal requires a fresh client read. Both initial
// preparation and pre-append checking must prove no submission, while unrelated
// metadata failures retain their own classification rather than being retried.
func TestPartialUpsertProofRouteRefusalIsUnsubmitted(t *testing.T) {
	for _, stage := range []string{"preparation", "append"} {
		for _, fault := range []string{"proof", "metadata_timeout", "metadata_unavailable"} {
			t.Run(stage+"/"+fault, func(t *testing.T) {
				metacache.MockWriteSchemaForTest(t)
				pre, post := oneShardSplit()
				fixture := newSplitFenceFixture(t, pre, post)
				task := fixture.upsertTask(t, []int64{10, 20, 30}, []int64{10, 20, 30})
				prepareMixedFenceTask(t, task, "relative")
				wal := newPartialUpdateCASTestWAL(t, 9)
				previous := streaming.WAL()
				streaming.SetWALForTest(wal)
				defer streaming.SetWALForTest(previous)
				stored := cloneFieldDataList(task.upsertMsg.InsertMsg.FieldsData)
				for _, field := range stored {
					if field.GetFieldName() == "tags" {
						for _, row := range field.GetScalars().GetArrayData().GetData() {
							row.GetLongData().Data = []int64{7}
						}
					}
				}
				cause := error(merr.ErrServiceUnavailable)
				if fault == "metadata_timeout" {
					cause = context.DeadlineExceeded
				}
				change := func() {
					if fault == "proof" {
						fixture.pre = post
					} else {
						fixture.infoErr = cause
					}
				}
				reads := 0
				query := mockey.Mock(retrieveByPKs).To(func(_ context.Context, _ *UpsertTask, _ *schemapb.IDs, _ []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
					reads++
					bindPartialUpdateCASTestReadTimestamps(t, task, uint64(1000+reads))
					if stage == "preparation" {
						change()
					}
					return &milvuspb.QueryResults{Status: merr.Success(), FieldsData: cloneFieldDataList(stored)}, segcore.StorageCost{}, nil
				}).Build()
				defer query.UnPatch()
				err := task.prepareUpsert(context.Background())
				if stage == "append" {
					require.NoError(t, err)
					change()
					err = task.Execute(context.Background())
				}
				require.Error(t, err)
				if fault == "proof" {
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
					require.EqualValues(t, 2, merr.Code(err))
					require.True(t, merr.Status(err).GetRetriable())
					require.Contains(t, err.Error(), "routing changed after its CAS proof")
					require.Greater(t, fixture.evictions, 0)
				} else {
					require.ErrorIs(t, err, cause)
					require.Equal(t, merr.Code(cause), merr.Code(err))
				}
				require.Equal(t, 1, reads, "the current contract returns to the client for a new read")
				require.Zero(t, wal.appendCalls)
				require.False(t, task.partialState.appendAttempted)
				require.Empty(t, task.partialState.written)
				require.False(t, task.canRetryPartialUpdateCASConflict(), "relative CAS attempts cannot enter a later reprepare after a WAL submission")
			})
		}
	}
}

// Cancellation can arrive while the actual packers finish against cached
// metadata. The final conditional submission gate must still prevent any append.
func TestConditionalUpsertCanceledAfterPackingDoesNotSubmit(t *testing.T) {
	task := partialUpdateCASRealPackTestTask(t, []int64{10, 20, 30}, []int64{10, 20, 30}, []int64{10, 20, 30})
	prepareMixedFenceTask(t, task, "conditional")
	wal := newPartialUpdateCASTestWAL(t, 9)
	previous := streaming.WAL()
	streaming.SetWALForTest(wal)
	defer streaming.SetWALForTest(previous)
	preparePartialUpdateCASTestGroups(t, task)
	partition := mockey.Mock((*metacache.MetaCache).GetPartitionID).Return(UniqueID(100), nil).Build()
	defer partition.UnPatch()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allocated := false
	allocation := mockey.Mock((*allocator.IDAllocator).Alloc).To(func(_ *allocator.IDAllocator, n uint32) (UniqueID, UniqueID, error) {
		allocated = true
		cancel()
		return 1000, 1000 + int64(n), nil
	}).Build()
	defer allocation.UnPatch()
	err := task.Execute(ctx)
	require.True(t, allocated, "cancellation must occur during real packing, after the outer retry admission")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, merr.CanceledCode, merr.Code(err))
	require.Zero(t, wal.appendCalls)
	require.False(t, task.partialState.appendAttempted)
	require.False(t, task.conditionalWriteState().TryAppend())
}

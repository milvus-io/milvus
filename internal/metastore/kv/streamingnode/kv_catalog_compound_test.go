package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestCatalog_SaveRecoverySnapshot(t *testing.T) {
	fullSnapshot := &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1},
		},
		VChannels: map[string]*streamingpb.VChannelMeta{
			"vch1": {Vchannel: "vch1"},
		},
		SalvageCheckpoint: &commonpb.ReplicateCheckpoint{ClusterId: "cluster1"},
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 42},
	}

	mockey.PatchConvey("success writes all parts with consume checkpoint last", t, func() {
		c := &catalog{}
		var calls []string
		mockey.Mock((*catalog).SaveSegmentAssignments).To(func(_ *catalog, _ context.Context, pchannel string, infos map[int64]*streamingpb.SegmentAssignmentMeta) error {
			assert.Equal(t, "pch1", pchannel)
			assert.Len(t, infos, 1)
			calls = append(calls, "SaveSegmentAssignments")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveVChannels).To(func(_ *catalog, _ context.Context, pchannel string, vchannels map[string]*streamingpb.VChannelMeta) error {
			assert.Equal(t, "pch1", pchannel)
			assert.Len(t, vchannels, 1)
			calls = append(calls, "SaveVChannels")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveSalvageCheckpoint).To(func(_ *catalog, _ context.Context, pchannel string, checkpoint *commonpb.ReplicateCheckpoint) error {
			assert.Equal(t, "cluster1", checkpoint.GetClusterId())
			calls = append(calls, "SaveSalvageCheckpoint")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveConsumeCheckpoint).To(func(_ *catalog, _ context.Context, pchannel string, checkpoint *streamingpb.WALCheckpoint) error {
			assert.Equal(t, uint64(42), checkpoint.GetTimeTick())
			calls = append(calls, "SaveConsumeCheckpoint")
			return nil
		}).Build()

		err := c.SaveRecoverySnapshot(context.TODO(), "pch1", fullSnapshot)
		assert.NoError(t, err)
		assert.Equal(t, []string{"SaveSegmentAssignments", "SaveVChannels", "SaveSalvageCheckpoint", "SaveConsumeCheckpoint"}, calls)
	})

	mockey.PatchConvey("nil snapshot and empty parts are skipped", t, func() {
		c := &catalog{}
		var calls []string
		mockey.Mock((*catalog).SaveSegmentAssignments).To(func(_ *catalog, _ context.Context, _ string, _ map[int64]*streamingpb.SegmentAssignmentMeta) error {
			calls = append(calls, "SaveSegmentAssignments")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveVChannels).To(func(_ *catalog, _ context.Context, _ string, _ map[string]*streamingpb.VChannelMeta) error {
			calls = append(calls, "SaveVChannels")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveSalvageCheckpoint).To(func(_ *catalog, _ context.Context, _ string, _ *commonpb.ReplicateCheckpoint) error {
			calls = append(calls, "SaveSalvageCheckpoint")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveConsumeCheckpoint).To(func(_ *catalog, _ context.Context, _ string, _ *streamingpb.WALCheckpoint) error {
			calls = append(calls, "SaveConsumeCheckpoint")
			return nil
		}).Build()

		err := c.SaveRecoverySnapshot(context.TODO(), "pch1", nil)
		assert.NoError(t, err)
		assert.Empty(t, calls)

		// initialize-style snapshot: only vchannels and consume checkpoint.
		err = c.SaveRecoverySnapshot(context.TODO(), "pch1", &metastore.WALRecoverySnapshot{
			VChannels:         map[string]*streamingpb.VChannelMeta{"vch1": {Vchannel: "vch1"}},
			ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 1},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"SaveVChannels", "SaveConsumeCheckpoint"}, calls)
	})

	mockey.PatchConvey("first step failure aborts the whole sequence", t, func() {
		c := &catalog{}
		var calls []string
		mockey.Mock((*catalog).SaveSegmentAssignments).To(func(_ *catalog, _ context.Context, _ string, _ map[int64]*streamingpb.SegmentAssignmentMeta) error {
			calls = append(calls, "SaveSegmentAssignments")
			return errors.New("save segment assignments failed")
		}).Build()
		mockey.Mock((*catalog).SaveVChannels).To(func(_ *catalog, _ context.Context, _ string, _ map[string]*streamingpb.VChannelMeta) error {
			calls = append(calls, "SaveVChannels")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveSalvageCheckpoint).To(func(_ *catalog, _ context.Context, _ string, _ *commonpb.ReplicateCheckpoint) error {
			calls = append(calls, "SaveSalvageCheckpoint")
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveConsumeCheckpoint).To(func(_ *catalog, _ context.Context, _ string, _ *streamingpb.WALCheckpoint) error {
			calls = append(calls, "SaveConsumeCheckpoint")
			return nil
		}).Build()

		err := c.SaveRecoverySnapshot(context.TODO(), "pch1", fullSnapshot)
		assert.Error(t, err)
		assert.Equal(t, []string{"SaveSegmentAssignments"}, calls)
	})

	mockey.PatchConvey("consume checkpoint failure is returned", t, func() {
		c := &catalog{}
		mockey.Mock((*catalog).SaveSegmentAssignments).To(func(_ *catalog, _ context.Context, _ string, _ map[int64]*streamingpb.SegmentAssignmentMeta) error {
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveVChannels).To(func(_ *catalog, _ context.Context, _ string, _ map[string]*streamingpb.VChannelMeta) error {
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveSalvageCheckpoint).To(func(_ *catalog, _ context.Context, _ string, _ *commonpb.ReplicateCheckpoint) error {
			return nil
		}).Build()
		mockey.Mock((*catalog).SaveConsumeCheckpoint).To(func(_ *catalog, _ context.Context, _ string, _ *streamingpb.WALCheckpoint) error {
			return errors.New("save consume checkpoint failed")
		}).Build()

		err := c.SaveRecoverySnapshot(context.TODO(), "pch1", fullSnapshot)
		assert.Error(t, err)
	})
}

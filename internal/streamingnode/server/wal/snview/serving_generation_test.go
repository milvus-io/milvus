package snview

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestServingIdentityRequiresExactLoadAndEpoch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		identity    servingIdentity
		load, epoch uint64
		match       bool
	}{
		{"master non-split", servingIdentity{}, 10, 0, true},
		{"missing identity", servingIdentity{}, 10, 2, false},
		{"missing load", servingIdentity{epoch: 2}, 10, 2, false},
		{"exact", servingIdentity{epoch: 2, loadVersion: 10}, 10, 2, true},
		{"same epoch reload", servingIdentity{epoch: 2, loadVersion: 10}, 11, 2, false},
		{"new epoch", servingIdentity{epoch: 2, loadVersion: 10}, 10, 3, false},
		{"full initial identity", servingIdentity{loadVersion: 10}, 10, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.match, tc.identity.matches(&viewpb.QueryViewMeta{LoadInfoVersion: tc.load, ShardSetEpoch: tc.epoch}))
		})
	}
}

func TestServingIdentityAcquisitionAndPendingDown(t *testing.T) {
	h, shard, entry, _, reports := leasedUpView(t, time.Minute)
	entry.sm.meta.LoadInfoVersion = 10
	entry.sm.meta.ShardSetEpoch = 2
	entry.sm.meta.SchemaVersion = 0
	schema := int32(0)
	lease, err := h.AcquireUpViewAtGeneration(t.Context(), shard.shardID, &schema, 2, 10)
	require.NoError(t, err)
	version := lease.Version
	lease.Release()
	_, err = h.AcquireUpViewAtGeneration(t.Context(), shard.shardID, &schema, 2, 11)
	require.True(t, viewerror.AsViewError(err).IsRetryable())
	_, err = h.AcquireUpViewAtGeneration(t.Context(), shard.shardID, &schema, 2, 0)
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	schema = 1
	_, err = h.AcquireUpViewAtGeneration(t.Context(), shard.shardID, &schema, 2, 10)
	require.True(t, viewerror.AsViewError(err).IsRetryable())
	schema = 0
	pushLeaseDown(h, reports)
	_, err = h.AcquireUpViewAtGeneration(t.Context(), shard.shardID, &schema, 2, 10)
	require.True(t, viewerror.AsViewError(err).IsRetryable(), "obsolete identity cannot renew through Phase 1")
	phase2, err := h.AcquireUpView(t.Context(), shard.shardID, version)
	require.NoError(t, err, "already planned work retains the normal execution lease")
	phase2.Release()
}

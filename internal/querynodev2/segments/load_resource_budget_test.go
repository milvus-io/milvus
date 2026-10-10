package segments

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadResourceBudgetSharedWithLegacyLoader(t *testing.T) {
	paramtable.Init()
	cfg := paramtable.Get()
	require.NoError(t, cfg.Save(cfg.QueryNodeCfg.TieredEvictionEnabled.Key, "false"))
	t.Cleanup(func() { cfg.Reset(cfg.QueryNodeCfg.TieredEvictionEnabled.Key) })
	require.NoError(t, cfg.Save(cfg.QueryNodeCfg.OverloadedMemoryThresholdPercentage.Key, "90"))
	t.Cleanup(func() { cfg.Reset(cfg.QueryNodeCfg.OverloadedMemoryThresholdPercentage.Key) })
	for _, p := range []*mockey.Mocker{
		mockey.Mock(hardware.GetMemoryCount).Return(uint64(1000)).Build(),
		mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(100)).Build(),
		mockey.Mock((*diskUsageFetcher).GetDiskUsage).Return(int64(0), nil).Build(),
		mockey.Mock(checkSegmentGpuMemSize).Return(nil).Build(),
		mockey.Mock((*segmentLoader).estimateSegmentLoadingResourceUsage).Return(&ResourceUsage{MemorySize: 600}, uint64(600), nil).Build(),
	} {
		patch := p
		t.Cleanup(func() { patch.UnPatch() })
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loader := NewLoader(ctx, nil, nil)
	legacy, err := loader.requestResource(ctx, &querypb.SegmentLoadInfo{SegmentID: 1})
	require.NoError(t, err)
	_, err = loader.Reserve(ctx, loadresource.SegmentResourceUsage{MemoryBytes: 300})
	require.Error(t, err, "both paths must account for the same pending load")
	loader.freeRequestResource(legacy)
	reservation, err := loader.Reserve(ctx, loadresource.SegmentResourceUsage{MemoryBytes: 300})
	require.NoError(t, err)
	require.EqualValues(t, 300, loader.committedResource.MemorySize)
	reservation.Release()
	reservation.Release()
	require.True(t, loader.committedResource.IsZero())
}

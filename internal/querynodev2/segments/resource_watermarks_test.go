package segments

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoaderResourceWatermarks(t *testing.T) {
	for _, tiered := range []bool{false, true} {
		t.Run(map[bool]string{false: "fallback", true: "tiered"}[tiered], func(t *testing.T) {
			loader := newResourceTestLoader(t, tiered, &ResourceUsage{})
			cfg := &paramtable.Get().QueryNodeCfg
			loader.committedResource = LoadResource{MemorySize: 100, DiskSize: 100}
			loader.duf.usage.Store(100)
			report := loader.GetResourceWatermarks()
			require.NotNil(t, report)
			memoryLimit, diskLimit := 1000.0, float64(cfg.DiskCapacityLimit.GetAsUint64())
			if !tiered {
				memoryLimit *= cfg.OverloadedMemoryThresholdPercentage.GetAsFloat()
				diskLimit *= cfg.MaxDiskUsagePercentage.GetAsFloat()
			}
			require.InDelta(t, 20000/memoryLimit, report.Memory, 1e-10)
			require.InDelta(t, 20000/diskLimit, report.Disk, 1e-10)
			loader.committedResource = LoadResource{MemorySize: 10000, DiskSize: cfg.DiskCapacityLimit.GetAsUint64()}
			report = loader.GetResourceWatermarks()
			require.Equal(t, 100.0, report.Memory)
			require.Equal(t, 100.0, report.Disk)
			loader.duf.err.Store(merr.ErrServiceUnavailable)
			require.Nil(t, loader.GetResourceWatermarks())
			loader.duf.err.Store(nil)
			loader.duf.usage.Store(-1)
			require.Nil(t, loader.GetResourceWatermarks())
			loader.duf.usage.Store(0)
			require.NoError(t, paramtable.Get().Save(cfg.DiskCapacityLimit.Key, "0"))
			require.Nil(t, loader.GetResourceWatermarks())
		})
	}
}

func TestLoaderAdmissionPreservesDimensionAndErrorCode(t *testing.T) {
	for _, reason := range []viewpb.SegmentPerparingFailure_Reason{viewpb.SegmentPerparingFailure_MemoryLimitExceeded, viewpb.SegmentPerparingFailure_DiskLimitExceeded} {
		t.Run(reason.String(), func(t *testing.T) {
			usage := &ResourceUsage{}
			loader := newResourceTestLoader(t, false, usage)
			if reason == viewpb.SegmentPerparingFailure_MemoryLimitExceeded {
				usage.MemorySize = 10000
			} else {
				usage.DiskSize = paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsUint64()
			}
			_, _, err := loader.ReserveLoadResourceWithStatus(t.Context(), nil, &querypb.SegmentLoadInfo{SegmentID: 1})
			require.ErrorIs(t, err, merr.ErrSegmentRequestResourceFailed, "existing merr contract is unchanged")
			var admission interface {
				ResourceFailureReason() viewpb.SegmentPerparingFailure_Reason
			}
			require.ErrorAs(t, err, &admission)
			require.Equal(t, reason, admission.ResourceFailureReason())
		})
	}
}

package segments

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// GetResourceWatermarks samples the same physical usage and committed ledger as
// requestResourceWithSchema. 100 means the applicable loader admission limit.
// It does not sample the native caching layer's separate reservation ledger.
func (loader *segmentLoader) GetResourceWatermarks() *viewpb.NodeResourceWatermarks {
	loader.mut.Lock()
	defer loader.mut.Unlock()
	disk, err := loader.duf.GetDiskUsage()
	if err != nil || disk < 0 {
		return nil
	}
	cfg := &paramtable.Get().QueryNodeCfg
	memoryLimit := float64(hardware.GetMemoryCount())
	diskLimit := float64(cfg.DiskCapacityLimit.GetAsUint64())
	if !cfg.TieredEvictionEnabled.GetAsBool() {
		memoryLimit *= min(1, cfg.OverloadedMemoryThresholdPercentage.GetAsFloat())
		diskLimit *= min(1, cfg.MaxDiskUsagePercentage.GetAsFloat())
	}
	if memoryLimit <= 0 || diskLimit <= 0 || math.IsNaN(memoryLimit) || math.IsNaN(diskLimit) {
		return nil
	}
	return &viewpb.NodeResourceWatermarks{
		Memory: min(100, 100*(float64(hardware.GetUsedMemoryCount())+float64(loader.committedResource.MemorySize))/memoryLimit),
		Disk:   min(100, 100*(float64(disk)+float64(loader.committedResource.DiskSize))/diskLimit),
	}
}

// resourceAdmissionError preserves the existing merr code while carrying the
// failing loader dimension to in-process QueryView preparation.
type resourceAdmissionError struct {
	error
	reason viewpb.SegmentPerparingFailure_Reason
}

func (e *resourceAdmissionError) Unwrap() error { return e.error }
func (e *resourceAdmissionError) ResourceFailureReason() viewpb.SegmentPerparingFailure_Reason {
	return e.reason
}

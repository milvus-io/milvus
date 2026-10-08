package segments

/*
#cgo pkg-config: milvus_core
#include "segcore/segment_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// LoadResourceBudget is shared by legacy and QueryView loading on one node.
// It owns admission state, never collection or segment registries.
type LoadResourceBudget struct {
	mut                       sync.Mutex
	committedResource         LoadResource
	committedLogicalResource  LoadResource
	committedResourceNotifier *syncutil.VersionedNotifier
	duf                       *diskUsageFetcher
}

func NewLoadResourceBudget(ctx context.Context) *LoadResourceBudget {
	duf := NewDiskUsageFetcher(ctx)
	go duf.Start()
	return &LoadResourceBudget{duf: duf, committedResourceNotifier: syncutil.NewVersionedNotifier()}
}

// LoadResourceReservation releases an admission exactly once.
type LoadResourceReservation struct {
	once    sync.Once
	release func()
}

func (r *LoadResourceReservation) Release() { r.once.Do(r.release) }
func (b *LoadResourceBudget) Reserve(ctx context.Context, usage loadresource.SegmentResourceUsage) (*LoadResourceReservation, error) {
	result, err := b.reserve(ctx, mlog.With(), &ResourceUsage{MemorySize: usage.MemoryBytes, DiskSize: usage.DiskBytes, MmapFieldCount: usage.MmapFieldCount, FieldGpuMemorySize: usage.FieldGPUMemoryBytes}, usage.MemoryBytes, 1)
	if err != nil {
		return nil, err
	}
	return &LoadResourceReservation{release: func() { b.freeRequestResource(result) }}, nil
}

func (loader *LoadResourceBudget) reserve(ctx context.Context, logger *mlog.Logger, loadingUsage *ResourceUsage, maxSegmentSize uint64, count int) (requestResourceResult, error) {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	physicalMemoryUsage := hardware.GetUsedMemoryCount()
	totalMemory := hardware.GetMemoryCount()

	physicalDiskUsage, err := loader.duf.GetDiskUsage()
	if err != nil {
		return requestResourceResult{}, merr.Wrap(err, "get local used size failed")
	}
	diskCap := paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsUint64()

	result := requestResourceResult{
		CommittedResource: loader.committedResource,
	}

	if loader.committedResource.MemorySize+physicalMemoryUsage >= totalMemory {
		return result, merr.WrapErrServiceMemoryLimitExceeded(float32(loader.committedResource.MemorySize+physicalMemoryUsage), float32(totalMemory))
	} else if loader.committedResource.DiskSize+uint64(physicalDiskUsage) >= diskCap {
		return result, merr.WrapErrServiceDiskLimitExceeded(float32(loader.committedResource.DiskSize+uint64(physicalDiskUsage)), float32(diskCap))
	}

	result.ConcurrencyLevel = funcutil.Min(hardware.GetCPUNum(), count)

	if err := loader.checkLoadingResource(ctx, logger, loadingUsage, maxSegmentSize, totalMemory, physicalMemoryUsage, physicalDiskUsage); err != nil {
		return result, err
	}

	result.Resource.MemorySize = loadingUsage.MemorySize
	result.Resource.DiskSize = loadingUsage.DiskSize
	// result.LogicalResource.MemorySize = lmu
	// result.LogicalResource.DiskSize = ldu

	loader.committedResource.Add(result.Resource)
	// loader.committedLogicalResource.Add(result.LogicalResource)
	mlog.Debug(ctx, "request resource for loading segments (unit in MiB)",
		mlog.Float64("memory", logutil.ToMB(float64(result.Resource.MemorySize))),
		mlog.Float64("committedMemory", logutil.ToMB(float64(loader.committedResource.MemorySize))),
		mlog.Float64("disk", logutil.ToMB(float64(result.Resource.DiskSize))),
		mlog.Float64("committedDisk", logutil.ToMB(float64(loader.committedResource.DiskSize))),
	)

	return result, nil
}

func (loader *LoadResourceBudget) freeRequestResource(requestResourceResult requestResourceResult) {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	resource := requestResourceResult.Resource
	// logicalResource := requestResourceResult.LogicalResource

	if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		C.ReleaseLoadingResource(C.CResourceUsage{
			memory_bytes: C.int64_t(resource.MemorySize),
			disk_bytes:   C.int64_t(resource.DiskSize),
		})
	}

	loader.committedResource.Sub(resource)
	// loader.committedLogicalResource.Sub(logicalResource)
	loader.committedResourceNotifier.NotifyAll()
}

func (loader *LoadResourceBudget) checkLoadingResource(
	ctx context.Context,
	logger *mlog.Logger,
	loadingUsage *ResourceUsage,
	maxSegmentSize uint64,
	totalMem uint64,
	memUsage uint64,
	localDiskUsage int64,
) error {
	memUsage += loader.committedResource.MemorySize
	if memUsage == 0 || totalMem == 0 {
		return merr.WrapErrServiceInternalMsg("get memory failed when checkLoadingResource")
	}

	diskUsage := uint64(localDiskUsage) + loader.committedResource.DiskSize
	predictMemUsage := memUsage + loadingUsage.MemorySize
	predictDiskUsage := diskUsage + loadingUsage.DiskSize

	logger.Debug(ctx, "predict memory and disk usage while loading (in MiB)",
		mlog.Float64("maxSegmentSize(MB)", logutil.ToMB(float64(maxSegmentSize))),
		mlog.Float64("committedMemSize(MB)", logutil.ToMB(float64(loader.committedResource.MemorySize))),
		mlog.Float64("memLimit(MB)", logutil.ToMB(float64(totalMem))),
		mlog.Float64("memUsage(MB)", logutil.ToMB(float64(memUsage))),
		mlog.Float64("committedDiskSize(MB)", logutil.ToMB(float64(loader.committedResource.DiskSize))),
		mlog.Float64("diskUsage(MB)", logutil.ToMB(float64(diskUsage))),
		mlog.Float64("predictMemUsage(MB)", logutil.ToMB(float64(predictMemUsage))),
		mlog.Float64("predictDiskUsage(MB)", logutil.ToMB(float64(predictDiskUsage))),
		mlog.Int("mmapFieldCount", loadingUsage.MmapFieldCount),
	)

	var loadingResource C.CResourceUsage
	reservedLoadingResource := false
	if paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		loadingResource = C.CResourceUsage{
			memory_bytes: C.int64_t(loadingUsage.MemorySize),
			disk_bytes:   C.int64_t(loadingUsage.DiskSize),
		}

		// try to reserve loading resource from caching layer
		if ok := C.TryReserveLoadingResourceWithTimeout(loadingResource, 1000); !ok {
			return merr.WrapErrSegmentRequestResourceFailed("memory/disk",
				fmt.Sprintf("failed to reserve loading resource from caching layer, predictMemUsage = %v MB, predictDiskUsage = %v MB, memUsage = %v MB, diskUsage = %v MB, memoryThresholdFactor = %f, diskThresholdFactor = %f",
					logutil.ToMB(float64(predictMemUsage)),
					logutil.ToMB(float64(predictDiskUsage)),
					logutil.ToMB(float64(memUsage)),
					logutil.ToMB(float64(diskUsage)),
					paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat(),
					paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat(),
				))
		}
		reservedLoadingResource = true
	} else {
		// fallback to original segment loading logic
		if predictMemUsage > uint64(float64(totalMem)*paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()) {
			mlog.Warn(ctx, "load segment failed, OOM if load",
				mlog.String("resourceType", "Memory"),
				mlog.Float64("maxSegmentSizeMB", logutil.ToMB(float64(maxSegmentSize))),
				mlog.Float64("memUsageMB", logutil.ToMB(float64(memUsage))),
				mlog.Float64("predictMemUsageMB", logutil.ToMB(float64(predictMemUsage))),
				mlog.Float64("totalMemMB", logutil.ToMB(float64(totalMem))),
				mlog.Float64("thresholdFactor", paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()),
			)
			return merr.WrapErrSegmentRequestResourceFailed("Memory")
		}

		if predictDiskUsage > uint64(float64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())*paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()) {
			mlog.Warn(ctx, "load segment failed, disk space is not enough",
				mlog.String("resourceType", "Disk"),
				mlog.Float64("diskUsageMB", logutil.ToMB(float64(diskUsage))),
				mlog.Float64("predictDiskUsageMB", logutil.ToMB(float64(predictDiskUsage))),
				mlog.Float64("totalDiskMB", logutil.ToMB(float64(uint64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())))),
				mlog.Float64("thresholdFactor", paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()),
			)
			return merr.WrapErrSegmentRequestResourceFailed("Disk")
		}
	}

	err := checkSegmentGpuMemSize(loadingUsage.FieldGpuMemorySize, float32(paramtable.Get().GpuConfig.OverloadedMemoryThresholdPercentage.GetAsFloat()))
	if err != nil {
		if reservedLoadingResource {
			C.ReleaseLoadingResource(loadingResource)
		}
		return err
	}

	return nil
}

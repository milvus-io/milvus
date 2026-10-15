package indexcgowrapper

/*
#cgo pkg-config: milvus_core

#include "indexbuilder/index_c.h"
#include "common/type_c.h"
*/
import "C"

import (
	"context"
	"runtime"
	"unsafe"

	"google.golang.org/protobuf/proto"

	_ "github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type CodecIndex interface {
	Delete() error
	UpLoad() (*cgopb.IndexStats, error)
}

// PreparedIndex retains the input loaded by LoadData until Build consumes it.
// Delete also releases input when a task is canceled before building.
type PreparedIndex interface {
	CodecIndex
	GetDownloadSize() (uint64, error)
	LoadData() error
	HasResidentInput() bool
	TryReserveBuild() (bool, error)
	SpillInput(context.Context) (bool, error)
	ReleaseInputMemory() error
	Build() error
}

var (
	_ CodecIndex    = (*CgoIndex)(nil)
	_ PreparedIndex = (*CgoIndex)(nil)
)

type CgoIndex struct {
	indexPtr      C.CIndex
	close         bool
	buildCtx      context.Context
	residentInput bool
}

func CreateIndex(ctx context.Context, buildIndexInfo *indexcgopb.BuildIndexInfo) (CodecIndex, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	index, err := createIndex(ctx, buildIndexInfo, true)
	if err != nil {
		return nil, err
	}
	if ctx != nil && ctx.Err() != nil {
		index.Delete()
		return nil, ctx.Err()
	}
	return index, nil
}

func CreateIndexBuilder(ctx context.Context, buildIndexInfo *indexcgopb.BuildIndexInfo) (PreparedIndex, error) {
	index, err := createIndex(ctx, buildIndexInfo, false)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func createIndex(ctx context.Context, buildIndexInfo *indexcgopb.BuildIndexInfo, build bool) (*CgoIndex, error) {
	buildIndexInfoBlob, err := proto.Marshal(buildIndexInfo)
	if err != nil {
		mlog.Warn(ctx, "marshal buildIndexInfo failed",
			mlog.String("clusterID", buildIndexInfo.GetClusterID()),
			mlog.FieldBuildID(buildIndexInfo.GetBuildID()),
			mlog.Err(err))
		return nil, err
	}
	var indexPtr C.CIndex
	data := (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(buildIndexInfoBlob)))
	size := C.uint64_t(len(buildIndexInfoBlob))
	var status C.CStatus
	if build {
		status = C.CreateIndex(&indexPtr, data, size)
	} else {
		status = C.CreateIndexBuilder(&indexPtr, data, size)
	}
	if err := HandleCStatus(&status, "failed to create index"); err != nil {
		return nil, err
	}

	index := &CgoIndex{
		indexPtr:      indexPtr,
		close:         false,
		buildCtx:      ctx,
		residentInput: true,
	}

	runtime.SetFinalizer(index, func(index *CgoIndex) {
		if index != nil && !index.close {
			mlog.Error(ctx, "there is leakage in index object, please check.")
		}
	})

	return index, nil
}

func (index *CgoIndex) GetDownloadSize() (uint64, error) {
	if index.close || index.indexPtr == nil {
		return 0, merr.WrapErrServiceInternalMsg("index builder is closed")
	}
	var bytes C.uint64_t
	status := C.GetIndexDownloadSize(index.indexPtr, &bytes)
	return uint64(bytes), HandleCStatus(&status, "failed to plan index download")
}

func (index *CgoIndex) LoadData() error {
	if index.close || index.indexPtr == nil {
		return merr.WrapErrServiceInternalMsg("index builder is closed")
	}
	residentInput := C.bool(true)
	status := C.LoadIndexData(index.indexPtr, &residentInput)
	index.residentInput = bool(residentInput)
	return HandleCStatus(&status, "failed to load index data")
}

func (index *CgoIndex) HasResidentInput() bool {
	return index.residentInput
}

func (index *CgoIndex) TryReserveBuild() (bool, error) {
	var admitted C.bool
	status := C.TryReserveIndexBuild(index.indexPtr, &admitted)
	return bool(admitted), HandleCStatus(&status, "failed to reserve index build")
}

func (index *CgoIndex) SpillInput(ctx context.Context) (bool, error) {
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		status := C.StopIndexSpill(index.indexPtr)
		_ = HandleCStatus(&status, "failed to stop index spill")
		close(stopped)
	})
	var complete C.bool
	status := C.SpillIndexInput(index.indexPtr, &complete)
	if !stop() {
		<-stopped
	}
	return bool(complete), HandleCStatus(&status, "failed to spill index input")
}

func (index *CgoIndex) ReleaseInputMemory() error {
	status := C.ReleaseIndexInputMemory(index.indexPtr)
	if err := HandleCStatus(&status, "failed to release index input"); err != nil {
		return err
	}
	index.residentInput = false
	return nil
}

func (index *CgoIndex) Build() error {
	if index.close || index.indexPtr == nil {
		return merr.WrapErrServiceInternalMsg("index builder is closed")
	}
	ctx := index.buildCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	status := C.BuildIndex(index.indexPtr)
	err := HandleCStatus(&status, "failed to build index")
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

type JSONKeyStatsResult struct {
	// MemSize is the actual memory size when loaded
	MemSize int64
	// Files maps file name to file size on disk
	Files map[string]int64
}

func CreateJSONKeyStats(ctx context.Context, buildIndexInfo *indexcgopb.BuildIndexInfo) (*JSONKeyStatsResult, error) {
	buildIndexInfoBlob, err := proto.Marshal(buildIndexInfo)
	if err != nil {
		mlog.Warn(ctx, "marshal buildIndexInfo failed",
			mlog.String("clusterID", buildIndexInfo.GetClusterID()),
			mlog.FieldBuildID(buildIndexInfo.GetBuildID()),
			mlog.Err(err))
		return nil, err
	}
	result := C.CreateProtoLayout()
	defer C.ReleaseProtoLayout(result)
	status := C.BuildJsonKeyIndex(result, (*C.uint8_t)(unsafe.Pointer(&buildIndexInfoBlob[0])), (C.uint64_t)(len(buildIndexInfoBlob)))
	if err := HandleCStatus(&status, "failed to build json key index"); err != nil {
		return nil, err
	}

	var indexStats cgopb.IndexStats
	if err := segcore.UnmarshalProtoLayout(result, &indexStats); err != nil {
		return nil, err
	}

	files := make(map[string]int64)
	var logSize int64
	for _, indexInfo := range indexStats.GetSerializedIndexInfos() {
		files[indexInfo.FileName] = indexInfo.FileSize
		logSize += indexInfo.FileSize
	}

	return &JSONKeyStatsResult{
		MemSize: indexStats.GetMemSize(),
		Files:   files,
	}, nil
}

func (index *CgoIndex) Delete() error {
	if index.close {
		return nil
	}
	status := C.DeleteIndex(index.indexPtr)
	index.close = true
	return HandleCStatus(&status, "failed to delete index")
}

func (index *CgoIndex) UpLoad() (*cgopb.IndexStats, error) {
	result := C.CreateProtoLayout()
	defer C.ReleaseProtoLayout(result)
	status := C.SerializeIndexAndUpLoad(index.indexPtr, result)
	if err := HandleCStatus(&status, "failed to serialize index and upload index"); err != nil {
		return nil, err
	}

	var indexStats cgopb.IndexStats
	if err := segcore.UnmarshalProtoLayout(result, &indexStats); err != nil {
		return nil, err
	}
	return &indexStats, nil
}

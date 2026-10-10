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
)

type CodecIndex interface {
	Delete() error
	UpLoad() (*cgopb.IndexStats, error)
}

var _ CodecIndex = (*CgoIndex)(nil)

type CgoIndex struct {
	indexPtr C.CIndex
	close    bool
}

func CreateIndex(ctx context.Context, buildIndexInfo *indexcgopb.BuildIndexInfo) (CodecIndex, error) {
	buildIndexInfoBlob, err := proto.Marshal(buildIndexInfo)
	if err != nil {
		mlog.Warn(ctx, "marshal buildIndexInfo failed",
			mlog.String("clusterID", buildIndexInfo.GetClusterID()),
			mlog.FieldBuildID(buildIndexInfo.GetBuildID()),
			mlog.Err(err))
		return nil, err
	}
	var indexPtr C.CIndex
	status := C.CreateIndex(&indexPtr, (*C.uint8_t)(unsafe.Pointer(&buildIndexInfoBlob[0])), (C.uint64_t)(len(buildIndexInfoBlob)))
	if err := HandleCStatus(&status, "failed to create index"); err != nil {
		return nil, err
	}

	index := &CgoIndex{
		indexPtr: indexPtr,
		close:    false,
	}

	runtime.SetFinalizer(index, func(index *CgoIndex) {
		if index != nil && !index.close {
			mlog.Error(ctx, "there is leakage in index object, please check.")
		}
	})

	return index, nil
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

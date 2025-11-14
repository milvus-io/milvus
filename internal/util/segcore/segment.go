package segcore

/*
#cgo pkg-config: milvus_core

#include "common/type_c.h"
#include "futures/future_c.h"
#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/cgo"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	SegmentTypeGrowing SegmentType = commonpb.SegmentState_Growing
	SegmentTypeSealed  SegmentType = commonpb.SegmentState_Sealed
)

type (
	SegmentType       = commonpb.SegmentState
	CSegmentInterface C.CSegmentInterface
)

// CreateCSegmentRequest is a request to create a segment.
type CreateCSegmentRequest struct {
	Collection  *CCollection
	SegmentID   int64
	SegmentType SegmentType
	IsSorted    bool
	LoadInfo    *querypb.SegmentLoadInfo
}

func (req *CreateCSegmentRequest) getCSegmentType() C.SegmentType {
	var segmentType C.SegmentType
	switch req.SegmentType {
	case SegmentTypeGrowing:
		segmentType = C.Growing
	case SegmentTypeSealed:
		segmentType = C.Sealed
	default:
		panic(fmt.Sprintf("invalid segment type: %d", req.SegmentType))
	}
	return segmentType
}

// CreateCSegment creates a segment from a CreateCSegmentRequest.
func CreateCSegment(req *CreateCSegmentRequest) (CSegment, error) {
	var ptr C.CSegmentInterface
	var status C.CStatus
	if req.LoadInfo != nil {
		segLoadInfo := ConvertToSegcoreSegmentLoadInfo(req.LoadInfo)
		loadInfoBlob, err := proto.Marshal(segLoadInfo)
		if err != nil {
			return nil, err
		}

		status = C.NewSegmentWithLoadInfo(req.Collection.rawPointer(), req.getCSegmentType(), C.int64_t(req.SegmentID), &ptr, C.bool(req.IsSorted), (*C.uint8_t)(unsafe.Pointer(&loadInfoBlob[0])), C.int64_t(len(loadInfoBlob)))
	} else {
		status = C.NewSegment(req.Collection.rawPointer(), req.getCSegmentType(), C.int64_t(req.SegmentID), &ptr, C.bool(req.IsSorted))
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &cSegmentImpl{id: req.SegmentID, ptr: ptr}, nil
}

// cSegmentImpl is a wrapper for cSegmentImplInterface.
type cSegmentImpl struct {
	id  int64
	ptr C.CSegmentInterface
}

// ID returns the ID of the segment.
func (s *cSegmentImpl) ID() int64 {
	return s.id
}

// RawPointer returns the raw pointer of the segment.
func (s *cSegmentImpl) RawPointer() CSegmentInterface {
	return CSegmentInterface(s.ptr)
}

// RowNum returns the number of rows in the segment.
func (s *cSegmentImpl) RowNum() int64 {
	rowCount := C.GetRealCount(s.ptr)
	return int64(rowCount)
}

// MemSize returns the memory size of the segment.
func (s *cSegmentImpl) MemSize() int64 {
	cMemSize := C.GetMemoryUsageInBytes(s.ptr)
	return int64(cMemSize)
}

// HasRawData checks if the segment has raw data.
func (s *cSegmentImpl) HasRawData(fieldID int64) bool {
	ret := C.HasRawData(s.ptr, C.int64_t(fieldID))
	return bool(ret)
}

// HasFieldData checks if the segment has field data.
func (s *cSegmentImpl) HasFieldData(fieldID int64) bool {
	ret := C.HasFieldData(s.ptr, C.int64_t(fieldID))
	return bool(ret)
}

// Search requests a search on the segment.
func (s *cSegmentImpl) Search(ctx context.Context, searchReq *SearchRequest) (*SearchResult, error) {
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(searchReq)

	future := cgo.Async(ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncSearch(
				traceCtx.ctx,
				s.ptr,
				searchReq.plan.cSearchPlan,
				searchReq.cPlaceholderGroup,
				C.uint64_t(searchReq.mvccTimestamp),
				C.int32_t(searchReq.consistencyLevel),
				C.uint64_t(searchReq.collectionTTL),
			))
		},
		cgo.WithName("search"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &SearchResult{cSearchResult: (C.CSearchResult)(result)}, nil
}

// Retrieve retrieves entities from the segment.
func (s *cSegmentImpl) Retrieve(ctx context.Context, plan *RetrievePlan) (*RetrieveResult, error) {
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(plan)
	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncRetrieve(
				traceCtx.ctx,
				s.ptr,
				plan.cRetrievePlan,
				C.uint64_t(plan.Timestamp),
				C.int64_t(plan.maxLimitSize),
				C.bool(plan.ignoreNonPk),
				C.int32_t(plan.consistencyLevel),
				C.uint64_t(plan.collectionTTL),
			))
		},
		cgo.WithName("retrieve"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &RetrieveResult{cRetrieveResult: (*C.CRetrieveResult)(result)}, nil
}

// RetrieveByOffsets retrieves entities from the segment by offsets.
func (s *cSegmentImpl) RetrieveByOffsets(ctx context.Context, plan *RetrievePlanWithOffsets) (*RetrieveResult, error) {
	if len(plan.Offsets) == 0 {
		return nil, merr.WrapErrParameterInvalid("segment offsets", "empty offsets")
	}

	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	defer runtime.KeepAlive(plan)
	defer runtime.KeepAlive(plan.Offsets)

	future := cgo.Async(
		ctx,
		func() cgo.CFuturePtr {
			return (cgo.CFuturePtr)(C.AsyncRetrieveByOffsets(
				traceCtx.ctx,
				s.ptr,
				plan.cRetrievePlan,
				(*C.int64_t)(unsafe.Pointer(&plan.Offsets[0])),
				C.int64_t(len(plan.Offsets)),
			))
		},
		cgo.WithName("retrieve-by-offsets"),
	)
	defer future.Release()
	result, err := future.BlockAndLeakyGet()
	if err != nil {
		return nil, err
	}
	return &RetrieveResult{cRetrieveResult: (*C.CRetrieveResult)(result)}, nil
}

// Insert inserts entities into the segment.
func (s *cSegmentImpl) Insert(ctx context.Context, request *InsertRequest) (*InsertResult, error) {
	offset, err := s.preInsert(len(request.RowIDs))
	if err != nil {
		return nil, err
	}

	insertRecordBlob, err := proto.Marshal(request.Record)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal insert record: %s", err)
	}

	numOfRow := len(request.RowIDs)
	cOffset := C.int64_t(offset)
	cNumOfRows := C.int64_t(numOfRow)
	cEntityIDsPtr := (*C.int64_t)(&(request.RowIDs)[0])
	cTimestampsPtr := (*C.uint64_t)(&(request.Timestamps)[0])

	status := C.Insert(s.ptr,
		cOffset,
		cNumOfRows,
		cEntityIDsPtr,
		cTimestampsPtr,
		(*C.uint8_t)(unsafe.Pointer(&insertRecordBlob[0])),
		(C.uint64_t)(len(insertRecordBlob)),
	)
	return &InsertResult{InsertedRows: int64(numOfRow)}, ConsumeCStatusIntoError(&status)
}

func (s *cSegmentImpl) preInsert(numOfRecords int) (int64, error) {
	var offset int64
	cOffset := (*C.int64_t)(&offset)
	status := C.PreInsert(s.ptr, C.int64_t(int64(numOfRecords)), cOffset)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return 0, err
	}
	return offset, nil
}

// Delete deletes entities from the segment.
func (s *cSegmentImpl) Delete(ctx context.Context, request *DeleteRequest) (*DeleteResult, error) {
	cSize := C.int64_t(request.PrimaryKeys.Len())
	cTimestampsPtr := (*C.uint64_t)(&(request.Timestamps)[0])

	ids, err := storage.ParsePrimaryKeysBatch2IDs(request.PrimaryKeys)
	if err != nil {
		return nil, err
	}

	dataBlob, err := proto.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ids: %s", err)
	}
	status := C.Delete(s.ptr,
		cSize,
		(*C.uint8_t)(unsafe.Pointer(&dataBlob[0])),
		(C.uint64_t)(len(dataBlob)),
		cTimestampsPtr,
	)
	return &DeleteResult{}, ConsumeCStatusIntoError(&status)
}

// LoadFieldData loads field data into the segment.
func (s *cSegmentImpl) LoadFieldData(ctx context.Context, request *LoadFieldDataRequest) (*LoadFieldDataResult, error) {
	creq, err := request.getCLoadFieldDataRequest()
	if err != nil {
		return nil, err
	}
	defer creq.Release()

	status := C.LoadFieldData(s.ptr, creq.cLoadFieldDataInfo)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "failed to load field data")
	}
	return &LoadFieldDataResult{}, nil
}

// AddFieldDataInfo adds field data info into the segment.
func (s *cSegmentImpl) AddFieldDataInfo(ctx context.Context, request *AddFieldDataInfoRequest) (*AddFieldDataInfoResult, error) {
	creq, err := request.getCLoadFieldDataRequest()
	if err != nil {
		return nil, err
	}
	defer creq.Release()

	status := C.AddFieldDataInfoForSealed(s.ptr, creq.cLoadFieldDataInfo)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "failed to add field data info")
	}
	return &AddFieldDataInfoResult{}, nil
}

// FinishLoad wraps up the load process and let segcore do the leftover jobs.
func (s *cSegmentImpl) FinishLoad() error {
	status := C.FinishLoad(s.ptr)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return errors.Wrap(err, "failed to finish load segment")
	}
	return nil
}

func (s *cSegmentImpl) Load(ctx context.Context) error {
	traceCtx := ParseCTraceContext(ctx)
	defer runtime.KeepAlive(traceCtx)
	status := C.SegmentLoad(traceCtx.ctx, s.ptr)
	return ConsumeCStatusIntoError(&status)
}

func (s *cSegmentImpl) DropIndex(ctx context.Context, fieldID int64) error {
	status := C.DropSealedSegmentIndex(s.ptr, C.int64_t(fieldID))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return errors.Wrap(err, "failed to drop index")
	}
	return nil
}

func (s *cSegmentImpl) DropJSONIndex(ctx context.Context, fieldID int64, nestedPath string) error {
	status := C.DropSealedSegmentJSONIndex(s.ptr, C.int64_t(fieldID), C.CString(nestedPath))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return errors.Wrap(err, "failed to drop json index")
	}
	return nil
}

// Release releases the segment.
func (s *cSegmentImpl) Release() {
	C.DeleteSegment(s.ptr)
}

// ConvertToSegcoreSegmentLoadInfo converts querypb.SegmentLoadInfo to segcorepb.SegmentLoadInfo.
// This function is needed because segcorepb.SegmentLoadInfo is a simplified version that doesn't
// depend on data_coord.proto and excludes fields like start_position, delta_position, and level.
func ConvertToSegcoreSegmentLoadInfo(src *querypb.SegmentLoadInfo) *segcorepb.SegmentLoadInfo {
	if src == nil {
		return nil
	}

	return &segcorepb.SegmentLoadInfo{
		SegmentID:        src.GetSegmentID(),
		PartitionID:      src.GetPartitionID(),
		CollectionID:     src.GetCollectionID(),
		DbID:             src.GetDbID(),
		FlushTime:        src.GetFlushTime(),
		BinlogPaths:      convertFieldBinlogs(src.GetBinlogPaths()),
		NumOfRows:        src.GetNumOfRows(),
		Statslogs:        convertFieldBinlogs(src.GetStatslogs()),
		Deltalogs:        convertFieldBinlogs(src.GetDeltalogs()),
		CompactionFrom:   src.GetCompactionFrom(),
		IndexInfos:       convertFieldIndexInfos(src.GetIndexInfos()),
		SegmentSize:      src.GetSegmentSize(),
		InsertChannel:    src.GetInsertChannel(),
		ReadableVersion:  src.GetReadableVersion(),
		StorageVersion:   src.GetStorageVersion(),
		IsSorted:         src.GetIsSorted(),
		TextStatsLogs:    convertTextIndexStats(src.GetTextStatsLogs()),
		Bm25Logs:         convertFieldBinlogs(src.GetBm25Logs()),
		JsonKeyStatsLogs: convertJSONKeyStats(src.GetJsonKeyStatsLogs()),
		Priority:         src.GetPriority(),
	}
}

// convertFieldBinlogs converts datapb.FieldBinlog to segcorepb.FieldBinlog.
func convertFieldBinlogs(src []*datapb.FieldBinlog) []*segcorepb.FieldBinlog {
	if src == nil {
		return nil
	}

	result := make([]*segcorepb.FieldBinlog, 0, len(src))
	for _, fb := range src {
		if fb == nil {
			continue
		}

		result = append(result, &segcorepb.FieldBinlog{
			FieldID:     fb.GetFieldID(),
			Binlogs:     convertBinlogs(fb.GetBinlogs()),
			ChildFields: fb.GetChildFields(),
		})
	}
	return result
}

// convertBinlogs converts datapb.Binlog to segcorepb.Binlog.
func convertBinlogs(src []*datapb.Binlog) []*segcorepb.Binlog {
	if src == nil {
		return nil
	}

	result := make([]*segcorepb.Binlog, 0, len(src))
	for _, b := range src {
		if b == nil {
			continue
		}

		result = append(result, &segcorepb.Binlog{
			EntriesNum:    b.GetEntriesNum(),
			TimestampFrom: b.GetTimestampFrom(),
			TimestampTo:   b.GetTimestampTo(),
			LogPath:       b.GetLogPath(),
			LogSize:       b.GetLogSize(),
			LogID:         b.GetLogID(),
			MemorySize:    b.GetMemorySize(),
		})
	}
	return result
}

// convertFieldIndexInfos converts querypb.FieldIndexInfo to segcorepb.FieldIndexInfo.
func convertFieldIndexInfos(src []*querypb.FieldIndexInfo) []*segcorepb.FieldIndexInfo {
	if src == nil {
		return nil
	}

	result := make([]*segcorepb.FieldIndexInfo, 0, len(src))
	for _, fii := range src {
		if fii == nil {
			continue
		}

		result = append(result, &segcorepb.FieldIndexInfo{
			FieldID:             fii.GetFieldID(),
			EnableIndex:         fii.GetEnableIndex(),
			IndexName:           fii.GetIndexName(),
			IndexID:             fii.GetIndexID(),
			BuildID:             fii.GetBuildID(),
			IndexParams:         fii.GetIndexParams(),
			IndexFilePaths:      fii.GetIndexFilePaths(),
			IndexSize:           fii.GetIndexSize(),
			IndexVersion:        fii.GetIndexVersion(),
			NumRows:             fii.GetNumRows(),
			CurrentIndexVersion: fii.GetCurrentIndexVersion(),
			IndexStoreVersion:   fii.GetIndexStoreVersion(),
		})
	}
	return result
}

// convertTextIndexStats converts datapb.TextIndexStats to segcorepb.TextIndexStats.
func convertTextIndexStats(src map[int64]*datapb.TextIndexStats) map[int64]*segcorepb.TextIndexStats {
	if src == nil {
		return nil
	}

	result := make(map[int64]*segcorepb.TextIndexStats, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}

		result[k] = &segcorepb.TextIndexStats{
			FieldID:    v.GetFieldID(),
			Version:    v.GetVersion(),
			Files:      v.GetFiles(),
			LogSize:    v.GetLogSize(),
			MemorySize: v.GetMemorySize(),
			BuildID:    v.GetBuildID(),
		}
	}
	return result
}

// convertJSONKeyStats converts datapb.JsonKeyStats to segcorepb.JsonKeyStats.
func convertJSONKeyStats(src map[int64]*datapb.JsonKeyStats) map[int64]*segcorepb.JsonKeyStats {
	if src == nil {
		return nil
	}

	result := make(map[int64]*segcorepb.JsonKeyStats, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}

		result[k] = &segcorepb.JsonKeyStats{
			FieldID:                v.GetFieldID(),
			Version:                v.GetVersion(),
			Files:                  v.GetFiles(),
			LogSize:                v.GetLogSize(),
			MemorySize:             v.GetMemorySize(),
			BuildID:                v.GetBuildID(),
			JsonKeyStatsDataFormat: v.GetJsonKeyStatsDataFormat(),
		}
	}
	return result
}

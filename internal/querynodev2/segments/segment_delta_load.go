package segments

/*
#cgo pkg-config: milvus_core
#include "segcore/segment_c.h"
*/
import "C"

import (
	"context"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// LoadSegmentDeletedRecords replays persisted deletes on the load pool. The
// caller owns the segment reference and serializes it with online Delete.
// LoadDeletedRecord must be used here: live Delete uses a different segcore path.
func LoadSegmentDeletedRecords(ctx context.Context, segment segcore.CSegment, data *storage.DeltaData) error {
	if data.DeleteRowCount() == 0 {
		return nil
	}
	pks, tss, rowNum := data.DeletePks(), data.DeleteTimestamps(), data.DeleteRowCount()
	ids, err := storage.ParsePrimaryKeysBatch2IDs(pks)
	if err != nil {
		return err
	}

	idsBlob, err := proto.Marshal(ids)
	if err != nil {
		return err
	}

	loadInfo := C.CLoadDeletedRecordInfo{
		timestamps:        unsafe.Pointer(&tss[0]),
		primary_keys:      (*C.uint8_t)(unsafe.Pointer(&idsBlob[0])),
		primary_keys_size: C.uint64_t(len(idsBlob)),
		row_count:         C.int64_t(rowNum),
	}
	/*
		CStatus
		LoadDeletedRecord(CSegmentInterface c_segment, CLoadDeletedRecordInfo deleted_record_info)
	*/
	var status C.CStatus
	// Delta-log replay during segment load runs on the load pool, not the
	// online-write mutate pool, so a large post-compaction replay cannot starve
	// online insert/delete (and thus tSafe advancement).
	GetLoadPool().Submit(func() (any, error) {
		start := time.Now()
		defer func() {
			metrics.QueryNodeCGOCallLatency.WithLabelValues(
				paramtable.GetStringNodeID(),
				"LoadDeletedRecord",
				"Sync",
			).Observe(float64(time.Since(start).Milliseconds()))
		}()
		status = C.LoadDeletedRecord(C.CSegmentInterface(segment.RawPointer()), loadInfo)
		return nil, nil
	}).Await()

	if err := HandleCStatus(ctx, &status, "LoadDeletedRecord failed",
		mlog.FieldSegmentID(segment.ID())); err != nil {
		return err
	}

	return nil
}

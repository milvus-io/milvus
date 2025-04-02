package segcore

/*
#cgo pkg-config: milvus_core
#include "segcore/load_field_data_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type RetrievePlanWithOffsets struct {
	*RetrievePlan
	Offsets []int64
}

type InsertRequest struct {
	RowIDs     []int64
	Timestamps []typeutil.Timestamp
	Record     *segcorepb.InsertRecord
}

type DeleteRequest struct {
	PrimaryKeys storage.PrimaryKeys
	Timestamps  []typeutil.Timestamp
}

type LoadFieldDataRequest struct {
	Fields   []LoadFieldDataInfo
	MMapDir  string
	RowCount int64
}

type LoadFieldDataInfo struct {
	Field      *datapb.FieldBinlog
	EnableMMap bool
}

func (req *LoadFieldDataRequest) getCLoadFieldDataRequest() (result *cLoadFieldDataRequest, err error) {
	var cLoadFieldDataInfo C.CLoadFieldDataInfo
	status := C.NewLoadFieldDataInfo(&cLoadFieldDataInfo)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "NewLoadFieldDataInfo failed")
	}
	defer func() {
		if err != nil {
			C.DeleteLoadFieldDataInfo(cLoadFieldDataInfo)
		}
	}()
	rowCount := C.int64_t(req.RowCount)

	for _, field := range req.Fields {
		cFieldID := C.int64_t(field.Field.GetFieldID())
		status = C.AppendLoadFieldInfo(cLoadFieldDataInfo, cFieldID, rowCount)
		if err := ConsumeCStatusIntoError(&status); err != nil {
			return nil, errors.Wrapf(err, "AppendLoadFieldInfo failed at fieldID, %d", field.Field.GetFieldID())
		}
		for _, binlog := range field.Field.Binlogs {
			cEntriesNum := C.int64_t(binlog.GetEntriesNum())
			cFile := C.CString(binlog.GetLogPath())
			defer C.free(unsafe.Pointer(cFile))

			status = C.AppendLoadFieldDataPath(cLoadFieldDataInfo, cFieldID, cEntriesNum, cFile)
			if err := ConsumeCStatusIntoError(&status); err != nil {
				return nil, errors.Wrapf(err, "AppendLoadFieldDataPath failed at binlog, %d, %s", field.Field.GetFieldID(), binlog.GetLogPath())
			}
		}

		C.EnableMmap(cLoadFieldDataInfo, cFieldID, C.bool(field.EnableMMap))
	}

	if len(req.MMapDir) > 0 {
		mmapDir := C.CString(req.MMapDir)
		defer C.free(unsafe.Pointer(mmapDir))
		C.AppendMMapDirPath(cLoadFieldDataInfo, mmapDir)
	}
	return &cLoadFieldDataRequest{
		cLoadFieldDataInfo: cLoadFieldDataInfo,
	}, nil
}

type cLoadFieldDataRequest struct {
	cLoadFieldDataInfo C.CLoadFieldDataInfo
}

func (req *cLoadFieldDataRequest) Release() {
	C.DeleteLoadFieldDataInfo(req.cLoadFieldDataInfo)
}

type AddFieldDataInfoRequest = LoadFieldDataRequest

type AddFieldDataInfoResult struct{}

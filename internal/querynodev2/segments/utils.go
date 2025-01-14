package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/segcore_init_c.h"
#include "common/init_c.h"

*/
import "C"

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var errLazyLoadTimeout = merr.WrapErrServiceInternal("lazy load time out")

func GetPkField(schema *schemapb.CollectionSchema) *schemapb.FieldSchema {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field
		}
	}
	return nil
}

// TODO: remove this function to proper file
// GetPrimaryKeys would get primary keys by insert messages
func GetPrimaryKeys(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	if msg.IsRowBased() {
		return getPKsFromRowBasedInsertMsg(msg, schema)
	}
	return getPKsFromColumnBasedInsertMsg(msg, schema)
}

func getPKsFromRowBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	offset := 0
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			break
		}
		switch field.DataType {
		case schemapb.DataType_Bool:
			offset++
		case schemapb.DataType_Int8:
			offset++
		case schemapb.DataType_Int16:
			offset += 2
		case schemapb.DataType_Int32:
			offset += 4
		case schemapb.DataType_Int64:
			offset += 8
		case schemapb.DataType_Float:
			offset += 4
		case schemapb.DataType_Double:
			offset += 8
		case schemapb.DataType_FloatVector:
			for _, t := range field.TypeParams {
				if t.Key == common.DimKey {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim * 4
					break
				}
			}
		case schemapb.DataType_BinaryVector:
			for _, t := range field.TypeParams {
				if t.Key == common.DimKey {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim / 8
					break
				}
			}
		case schemapb.DataType_Float16Vector:
			for _, t := range field.TypeParams {
				if t.Key == common.DimKey {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim * 2
					break
				}
			}
		case schemapb.DataType_BFloat16Vector:
			for _, t := range field.TypeParams {
				if t.Key == common.DimKey {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim * 2
					break
				}
			}
		case schemapb.DataType_SparseFloatVector:
			return nil, fmt.Errorf("SparseFloatVector not support in row based message")
		}
	}

	log.Info(strconv.FormatInt(int64(offset), 10))
	blobReaders := make([]io.Reader, len(msg.RowData))
	for i, blob := range msg.RowData {
		blobReaders[i] = bytes.NewReader(blob.GetValue()[offset : offset+8])
	}
	pks := make([]storage.PrimaryKey, len(blobReaders))

	for i, reader := range blobReaders {
		var int64PkValue int64
		err := binary.Read(reader, common.Endian, &int64PkValue)
		if err != nil {
			log.Warn("binary read blob value failed", zap.Error(err))
			return nil, err
		}
		pks[i] = storage.NewInt64PrimaryKey(int64PkValue)
	}

	return pks, nil
}

func getPKsFromColumnBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	primaryFieldData, err := typeutil.GetPrimaryFieldData(msg.GetFieldsData(), primaryFieldSchema)
	if err != nil {
		return nil, err
	}

	pks, err := storage.ParseFieldData2PrimaryKeys(primaryFieldData)
	if err != nil {
		return nil, err
	}

	return pks, nil
}

// mergeRequestCost merge the costs of request, the cost may came from different worker in same channel
// or different channel in same collection, for now we just choose the part with the highest response time
func mergeRequestCost(requestCosts []*internalpb.CostAggregation) *internalpb.CostAggregation {
	var result *internalpb.CostAggregation
	for _, cost := range requestCosts {
		if result == nil || result.ResponseTime < cost.ResponseTime {
			result = cost
		}
	}

	return result
}

func getIndexEngineVersion() (minimal, current int32) {
	GetDynamicPool().Submit(func() (any, error) {
		cMinimal, cCurrent := C.GetMinimalIndexVersion(), C.GetCurrentIndexVersion()
		minimal, current = int32(cMinimal), int32(cCurrent)
		return nil, nil
	}).Await()
	return minimal, current
}

// getSegmentMetricLabel returns the label for segment metrics.
func getSegmentMetricLabel(segment Segment) metricsutil.SegmentLabel {
	return metricsutil.SegmentLabel{
		DatabaseName:  segment.DatabaseName(),
		ResourceGroup: segment.ResourceGroup(),
	}
}

func FilterZeroValuesFromSlice(intVals []int64) []int64 {
	var result []int64
	for _, value := range intVals {
		if value != 0 {
			result = append(result, value)
		}
	}
	return result
}

// withLazyLoadTimeoutContext returns a new context with lazy load timeout.
func withLazyLoadTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	lazyLoadTimeout := paramtable.Get().QueryNodeCfg.LazyLoadWaitTimeout.GetAsDuration(time.Millisecond)
	// TODO: use context.WithTimeoutCause instead of contextutil.WithTimeoutCause in go1.21
	return contextutil.WithTimeoutCause(ctx, lazyLoadTimeout, errLazyLoadTimeout)
}

func GetSegmentRelatedDataSize(segment Segment) int64 {
	if segment.Type() == SegmentTypeSealed {
		return calculateSegmentLogSize(segment.LoadInfo())
	}
	return segment.MemSize()
}

func calculateSegmentLogSize(segmentLoadInfo *querypb.SegmentLoadInfo) int64 {
	segmentSize := int64(0)

	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
		segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
	}

	// Get size of state data
	for _, fieldBinlog := range segmentLoadInfo.Statslogs {
		segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
	}

	// Get size of delete data
	for _, fieldBinlog := range segmentLoadInfo.Deltalogs {
		segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
	}

	return segmentSize
}

func getFieldSizeFromFieldBinlog(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.LogSize
	}

	return fieldSize
}

func getFieldSchema(schema *schemapb.CollectionSchema, fieldID int64) (*schemapb.FieldSchema, error) {
	for _, field := range schema.Fields {
		if field.FieldID == fieldID {
			return field, nil
		}
	}
	return nil, fmt.Errorf("field %d not found in schema", fieldID)
}

func isIndexMmapEnable(fieldSchema *schemapb.FieldSchema, indexInfo *querypb.FieldIndexInfo) bool {
	enableMmap, exist := common.IsMmapIndexEnabled(indexInfo.IndexParams...)
	if exist {
		return enableMmap
	}
	indexType := common.GetIndexType(indexInfo.IndexParams)
	var indexSupportMmap bool
	var defaultEnableMmap bool
	if typeutil.IsVectorType(fieldSchema.GetDataType()) {
		indexSupportMmap = vecindexmgr.GetVecIndexMgrInstance().IsMMapSupported(indexType)
		defaultEnableMmap = params.Params.QueryNodeCfg.MmapVectorIndex.GetAsBool()
	} else {
		indexSupportMmap = indexparamcheck.IsScalarMmapIndex(indexType)
		defaultEnableMmap = params.Params.QueryNodeCfg.MmapScalarIndex.GetAsBool()
	}
	return indexSupportMmap && defaultEnableMmap
}

func isDataMmapEnable(fieldSchema *schemapb.FieldSchema) bool {
	enableMmap, exist := common.IsMmapDataEnabled(fieldSchema.GetTypeParams()...)
	if exist {
		return enableMmap
	}
	if typeutil.IsVectorType(fieldSchema.GetDataType()) {
		return params.Params.QueryNodeCfg.MmapVectorField.GetAsBool()
	}
	return params.Params.QueryNodeCfg.MmapScalarField.GetAsBool()
}

func isGrowingMmapEnable() bool {
	return params.Params.QueryNodeCfg.GrowingMmapEnabled.GetAsBool()
}

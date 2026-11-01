package queryresource

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SearchOptimizationInfo borrows the selected View's metadata during planning.
// Only scalar tuning inputs cross the wire, never a Schema or segment row array.
func (r *ReadRuntime) SearchOptimizationInfo(fieldID int64, count uint32) (*viewpb.SearchOptimizationInfo, error) {
	field := typeutil.GetField(r.Schema(), fieldID)
	if field == nil {
		return nil, merr.WrapErrServiceInternalMsg("search optimization field %d is missing from view schema", fieldID)
	}
	info := &viewpb.SearchOptimizationInfo{SealedSegmentCount: count}
	if field.GetDataType() != schemapb.DataType_SparseFloatVector {
		dim, err := typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
		info.VectorDim = dim
	}
	for _, index := range r.schema.collection.IndexMeta().GetIndexMetas() {
		if index.GetFieldID() == fieldID {
			info.IndexType = common.GetIndexType(index.GetIndexParams())
			break
		}
	}
	return info, nil
}

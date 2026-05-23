package funcutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReorderFieldDataByInputIDs returns a new fields-data slice whose rows are
// laid out in the order of inputIDs.
//
// The retrieve / query pipeline always merges per-segment results by primary
// key ascending (see typeutil.SelectMinPK), so when callers fetch rows by a
// list of primary keys (e.g. proxy's "search by PK" path) the rows come back
// sorted by PK, not in the order the caller asked for. Search uses each row
// in Nq order, so the mismatch corrupts per-query results.
//
// pkFieldData must be the PK field already present in src and must have the
// same row count as src. Every input ID must exist in pkFieldData — duplicate
// and missing-ID handling is the caller's responsibility.
func ReorderFieldDataByInputIDs(
	src []*schemapb.FieldData,
	pkFieldData *schemapb.FieldData,
	inputIDs *schemapb.IDs,
) ([]*schemapb.FieldData, error) {
	if pkFieldData == nil {
		return nil, fmt.Errorf("primary key field data is nil")
	}

	pkToRow := make(map[any]int64)
	switch pkFieldData.GetType() {
	case schemapb.DataType_Int64:
		for i, pk := range pkFieldData.GetScalars().GetLongData().GetData() {
			pkToRow[pk] = int64(i)
		}
	case schemapb.DataType_VarChar:
		for i, pk := range pkFieldData.GetScalars().GetStringData().GetData() {
			pkToRow[pk] = int64(i)
		}
	default:
		return nil, fmt.Errorf("unsupported primary key data type: %s", pkFieldData.GetType())
	}

	n := int64(typeutil.GetSizeOfIDs(inputIDs))
	dst := typeutil.PrepareResultFieldData(src, n)
	idxComputer := typeutil.NewFieldDataIdxComputer(src)

	appendByID := func(id any) error {
		rowIdx, ok := pkToRow[id]
		if !ok {
			return fmt.Errorf("primary key %v not found in query result", id)
		}
		fieldIdxs := idxComputer.Compute(rowIdx)
		typeutil.AppendFieldData(dst, src, rowIdx, fieldIdxs...)
		return nil
	}

	switch inputIDs.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		for _, id := range inputIDs.GetIntId().GetData() {
			if err := appendByID(id); err != nil {
				return nil, err
			}
		}
	case *schemapb.IDs_StrId:
		for _, id := range inputIDs.GetStrId().GetData() {
			if err := appendByID(id); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("unsupported input ID field type")
	}

	return dst, nil
}

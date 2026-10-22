package materialize

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A batch's leading columns: the pk, then one $sys:<field id> column per
// system field (ParseSchema finds them by name).
const (
	pkColumnName  = "$pk"
	sysColumnName = "$sys:"
)

// BuildBatch turns the worker's reduce output into one Materialize batch: the
// pk from header.Ids, then the system columns segcore kept in the header, then
// every gathered Arrow column. It returns nil when the header holds no rows.
// A user column left in the header means a field segcore does not carry as
// Arrow; the request must then fall back to Query (spec §4).
func BuildBatch(header *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection) (arrow.Record, error) {
	// A batch has no place for ElementIndices; an element-level result must
	// go back to Query rather than lose them (spec, Scope).
	if header.GetElementLevel() || len(header.GetElementIndices()) > 0 {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "materialize: element-level results are not supported")
	}
	rows := typeutil.GetSizeOfIDs(header.GetIds())
	if rows == 0 {
		return nil, nil
	}
	if sel == nil || sel.Empty() {
		return nil, merr.WrapErrServiceInternalMsg("materialize: %d rows but no Arrow selection", rows)
	}
	for _, fd := range header.GetFieldsData() {
		id := fd.GetFieldId()
		if (id != common.RowIDField && id != common.TimeStampField) || fd.GetType() != schemapb.DataType_Int64 {
			return nil, merr.Wrapf(merr.ErrServiceUnimplemented, "materialize: field %d (%s) is not carried as Arrow", id, fd.GetType())
		}
		if len(fd.GetScalars().GetLongData().GetData()) != rows {
			return nil, merr.WrapErrServiceInternalMsg("materialize: system field %d has %d rows, header has %d",
				id, len(fd.GetScalars().GetLongData().GetData()), rows)
		}
	}

	user, err := queryutil.GatherArrowSelection(sel)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, merr.WrapErrServiceInternalMsg("materialize: Arrow selection gathered no record")
	}
	defer user.Release()
	if int(user.NumRows()) != rows {
		return nil, merr.WrapErrServiceInternalMsg("materialize: header has %d rows, Arrow selection %d", rows, user.NumRows())
	}

	mem := memory.DefaultAllocator
	fields := make([]arrow.Field, 0, 1+len(header.GetFieldsData())+int(user.NumCols()))
	cols := make([]arrow.Array, 0, cap(fields))
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	pkArr, pkType, err := idsToArrow(mem, header.GetIds())
	if err != nil {
		return nil, err
	}
	fields = append(fields, arrow.Field{Name: pkColumnName, Type: pkType})
	cols = append(cols, pkArr)
	for _, fd := range header.GetFieldsData() {
		b := array.NewInt64Builder(mem)
		b.AppendValues(fd.GetScalars().GetLongData().GetData(), nil)
		cols = append(cols, b.NewArray())
		b.Release()
		fields = append(fields, arrow.Field{Name: sysColumnName + strconv.FormatInt(fd.GetFieldId(), 10), Type: arrow.PrimitiveTypes.Int64})
	}
	for i, f := range user.Schema().Fields() {
		fields = append(fields, f)
		col := user.Column(i)
		col.Retain()
		cols = append(cols, col)
	}
	// One schema per layout, so EncodeBatch can reuse a writer that already
	// wrote its Schema message (encode_cache.go).
	// NewRecord retains the columns; the deferred releases drop our references.
	return array.NewRecord(batchLayouts.schemaFor(fields, user.Schema().Metadata()), cols, int64(rows)), nil
}

func idsToArrow(mem memory.Allocator, ids *schemapb.IDs) (arrow.Array, arrow.DataType, error) {
	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		b.AppendValues(ids.GetIntId().GetData(), nil)
		return b.NewArray(), arrow.PrimitiveTypes.Int64, nil
	case *schemapb.IDs_StrId:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.AppendValues(ids.GetStrId().GetData(), nil)
		return b.NewArray(), arrow.BinaryTypes.String, nil
	default:
		return nil, nil, merr.WrapErrServiceInternalMsg("materialize: unsupported pk ids %T", ids.GetIdField())
	}
}

package dql

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/search_agg"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func reduceSearchAggregationRows(ctx context.Context, results []*internalpb.SearchResults, nq, topK, offset, groupSize int64, fields []int64, metricType string, pkType schemapb.DataType, collectionID int64, partitionIDs []int64) (*search_agg.ReducedRows, error) {
	ctx, span := otel.Tracer(typeutil.ProxyRole).Start(ctx, "reduceResults")
	defer span.End()
	sources, err := decodeSearchResults(ctx, results)
	if err != nil {
		mlog.Warn(ctx, "failed to decode search results", mlog.Err(err))
		return nil, err
	}
	rows := &search_agg.ReducedRows{Sources: sources, GroupFieldIDs: fields, NegateScores: !metric.PositivelyRelated(metricType)}
	if len(sources) == 0 {
		mlog.Debug(ctx, "reduced search results is empty, fill in empty result")
		rows.Topks = make([]int64, nq)
		return rows, nil
	}
	mlog.Debug(ctx, "proxy search post execute reduce",
		mlog.Int64("collection", collectionID),
		mlog.Int64s("partitionIDs", partitionIDs),
		mlog.Int("number of valid search results", len(sources)))
	selection := &aggregationSelection{rows: rows, sizes: make([]aggregationRowSizer, len(sources))}
	for i, source := range sources {
		selection.sizes[i] = newAggregationRowSizer(source.GetFieldsData())
	}
	result, err := reduceGroupBy(ctx, sources, nq, topK, metricType, pkType, offset, groupSize, fields, true, selection)
	if err != nil {
		mlog.Warn(ctx, "failed to reduce search results", mlog.Err(err))
		return nil, err
	}
	rows.Topks = result.GetResults().GetTopks()
	rows.AllSearchCount = result.GetResults().GetAllSearchCount()
	return rows, nil
}

type aggregationSelection struct {
	rows  *search_agg.ReducedRows
	sizes []aggregationRowSizer
}

func (s *aggregationSelection) addGroup(values []any) int {
	id := len(s.rows.GroupKeys)
	s.rows.GroupKeys = append(s.rows.GroupKeys, values)
	return id
}

func (s *aggregationSelection) append(ref reduce.RowRef, groupID int) int64 {
	s.rows.GroupIDs = append(s.rows.GroupIDs, groupID)
	return s.sizes[ref.ResultIdx].size(int(ref.RowIdx))
}

// This preserves AppendFieldData's existing quota charge, which is not the
// serialized payload size: scalar values use their Go storage width, dense
// vectors charge one slice header per non-null row, and sparse vectors charge
// their encoded bytes. Changing that policy belongs to a separate change.
type aggregationRowSizer struct {
	fixed         int64
	nullableDense [][]bool
	sparse        []func(int) any
}

func newAggregationRowSizer(fields []*schemapb.FieldData) aggregationRowSizer {
	var s aggregationRowSizer
	const word = int64(strconv.IntSize / 8)
	for _, field := range fields {
		switch data := field.GetField().(type) {
		case *schemapb.FieldData_Scalars:
			switch data.Scalars.GetData().(type) {
			case *schemapb.ScalarField_BoolData:
				s.fixed++
			case *schemapb.ScalarField_IntData, *schemapb.ScalarField_FloatData:
				s.fixed += 4
			case *schemapb.ScalarField_LongData, *schemapb.ScalarField_DoubleData, *schemapb.ScalarField_TimestamptzData:
				s.fixed += 8
			case *schemapb.ScalarField_StringData:
				s.fixed += 2 * word
			case *schemapb.ScalarField_ArrayData:
				s.fixed += word
			case *schemapb.ScalarField_JsonData, *schemapb.ScalarField_GeometryData:
				s.fixed += 3 * word
			}
		case *schemapb.FieldData_Vectors:
			switch data.Vectors.GetData().(type) {
			case *schemapb.VectorField_SparseFloatVector:
				s.sparse = append(s.sparse, typeutil.GetDataIterator(field))
			case *schemapb.VectorField_FloatVector, *schemapb.VectorField_BinaryVector,
				*schemapb.VectorField_Float16Vector, *schemapb.VectorField_Bfloat16Vector, *schemapb.VectorField_Int8Vector:
				if valid := typeutil.GetFieldDataValidData(field); len(valid) > 0 {
					s.nullableDense = append(s.nullableDense, valid)
				} else {
					s.fixed += 3 * word
				}
			}
		}
	}
	return s
}

func (s *aggregationRowSizer) size(row int) int64 {
	result := s.fixed
	for _, valid := range s.nullableDense {
		if valid[row] {
			result += 3 * int64(strconv.IntSize/8)
		}
	}
	for _, read := range s.sparse {
		if value := read(row); value != nil {
			result += int64(len(value.([]byte)))
		}
	}
	return result
}

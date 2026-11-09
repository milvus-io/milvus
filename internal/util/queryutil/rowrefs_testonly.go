//go:build test

package queryutil

import (
	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// RowRefsForTest builds selection rows from (resultIdx, rowIdx) pairs for
// tests in other packages; rowRef itself stays unexported.
func RowRefsForTest(pairs [][2]int64) []rowRef {
	out := make([]rowRef, len(pairs))
	for i, p := range pairs {
		out[i] = rowRef{resultIdx: int(p[0]), rowIdx: p[1]}
	}
	return out
}

// ResultSizeForTest is what the maxOutputSize guard (rowSizeCalculator)
// charges for every row of a result whose FieldsData is fields and whose
// user columns are rec, for tests that pin another size rule to it.
func ResultSizeForTest(fields []*schemapb.FieldData, rec arrow.Record) int64 {
	c := newRowSizeCalculator(&internalpb.RetrieveResults{FieldsData: fields}).withArrowRecord(rec)
	var size int64
	for r := int64(0); r < rec.NumRows(); r++ {
		size += c.rowSize(r)
	}
	return size
}

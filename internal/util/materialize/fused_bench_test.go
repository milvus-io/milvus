package materialize

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

// benchSelection builds a worker's reduce output as the pipeline benchmark
// does: rows scattered over 8 segment records three times their share, user
// columns pk (int64) plus either a 768-dim float vector or a ~300-byte text
// and a ~150-byte JSON.
func benchSelection(b *testing.B, fields string, rows int) (*segcorepb.RetrieveResults, *queryutil.ArrowSelection) {
	const segs = 8
	r := rand.New(rand.NewSource(int64(rows)))
	afs := []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}}
	order := []string{"1", "100"}
	if fields == "vec768" {
		afs = append(afs, arrow.Field{Name: "101", Type: &arrow.FixedSizeBinaryType{ByteWidth: 768 * 4}})
		order = append(order, "101")
	} else {
		afs = append(afs, arrow.Field{Name: "102", Type: arrow.BinaryTypes.String}, arrow.Field{Name: "103", Type: arrow.BinaryTypes.Binary})
		order = append(order, "102", "103")
	}
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"}, []string{strings.Join(order, ","), ""})
	schema := arrow.NewSchema(afs, &md)
	perSeg := 3*rows/segs + 1
	recs := make([]arrow.Record, segs)
	vec := make([]byte, 768*4)
	for s := range recs {
		rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for row := 0; row < perSeg; row++ {
			rb.Field(0).(*array.Int64Builder).Append(int64(s*perSeg + row))
			if fields == "vec768" {
				r.Read(vec)
				rb.Field(1).(*array.FixedSizeBinaryBuilder).Append(vec)
			} else {
				rb.Field(1).(*array.StringBuilder).Append(strings.Repeat("w", 250+r.Intn(100)))
				rb.Field(2).(*array.BinaryBuilder).Append([]byte(`{"tag":"x","note":"` + strings.Repeat("n", 100+r.Intn(50)) + `"}`))
			}
		}
		recs[s] = rb.NewRecord()
		rb.Release()
	}
	b.Cleanup(func() {
		for _, rec := range recs {
			rec.Release()
		}
	})
	slots := r.Perm(segs * perSeg)[:rows]
	pairs := make([][2]int64, rows)
	pks := make([]int64, rows)
	ts := make([]int64, rows)
	for i, slot := range slots {
		pairs[i] = [2]int64{int64(slot / perSeg), int64(slot % perSeg)}
		pks[i] = int64(slot)
		ts[i] = 100
	}
	header := &segcorepb.RetrieveResults{Ids: intIDs(pks...), FieldsData: []*schemapb.FieldData{tsField(ts...)}}
	return header, &queryutil.ArrowSelection{Records: recs, Rows: queryutil.RowRefsForTest(pairs)}
}

// BenchmarkEncodeWorker compares the two-step path (BuildBatch, EncodeBatch,
// RawSize) with the fused writer on one worker's batch.
func BenchmarkEncodeWorker(b *testing.B) {
	shapes := []struct {
		fields string
		rows   int
	}{{"text", 3}, {"text", 100}, {"text", 1000}, {"vec768", 1000}}
	for _, s := range shapes {
		header, sel := benchSelection(b, s.fields, s.rows)
		want := twoStep(b, header, sel)
		got, ok, err := encodeFused(header, sel)
		require.NoError(b, err)
		require.True(b, ok)
		require.Equal(b, want, got)
		b.Run(fmt.Sprintf("fields=%s/rows=%s/enc=twostep", s.fields, strconv.Itoa(s.rows)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rec, err := BuildBatch(header, sel)
				if err != nil {
					b.Fatal(err)
				}
				if _, _, err := EncodeBatch(rec, CodecNone); err != nil {
					b.Fatal(err)
				}
				_ = RawSize(rec)
				rec.Release()
			}
		})
		b.Run(fmt.Sprintf("fields=%s/rows=%s/enc=fused", s.fields, strconv.Itoa(s.rows)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, ok, err := encodeFused(header, sel); err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})
	}
}

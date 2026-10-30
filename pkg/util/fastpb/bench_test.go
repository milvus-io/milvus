package fastpb

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// buildVarcharSRD builds a varchar-PK + varchar-output SearchResultData (rows = nq*topk).
func buildVarcharSRD(rows, strLen int) *schemapb.SearchResultData {
	ids := make([]string, rows)
	for i := range ids {
		ids[i] = fmt.Sprintf("pk_%0*d", strLen, i)
	}
	scores := make([]float32, rows)
	for i := range scores {
		scores[i] = float32(i) * 0.5
	}
	return &schemapb.SearchResultData{
		NumQueries: 10, TopK: int64(rows / 10), PrimaryFieldName: "pk",
		Scores:     scores,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}},
		FieldsData: varcharFieldData(rows, strLen),
	}
}

// buildVectorSRD builds an int64-PK + float-vector SearchResultData.
func buildVectorSRD(rows, dim int) *schemapb.SearchResultData {
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	scores := make([]float32, rows)
	return &schemapb.SearchResultData{
		NumQueries: 10, TopK: int64(rows / 10), PrimaryFieldName: "id",
		Scores:     scores,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
		FieldsData: vectorFieldData(rows, dim),
	}
}

func benchSRD(b *testing.B, src *schemapb.SearchResultData) {
	wire, err := proto.Marshal(src)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("official", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out schemapb.SearchResultData
			if err := proto.Unmarshal(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fastpb", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out schemapb.SearchResultData
			if err := UnmarshalSearchResultData(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSRD_Varchar(b *testing.B) { benchSRD(b, buildVarcharSRD(1000, 12)) }
func BenchmarkSRD_Vector(b *testing.B)  { benchSRD(b, buildVectorSRD(1000, 768)) }

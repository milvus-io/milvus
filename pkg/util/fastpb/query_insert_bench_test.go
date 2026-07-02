package fastpb

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func varcharFieldData(rows, strLen int) []*schemapb.FieldData {
	titles := make([]string, rows)
	for i := range titles {
		titles[i] = fmt.Sprintf("title_%0*d", strLen, i)
	}
	return []*schemapb.FieldData{{
		Type: schemapb.DataType_VarChar, FieldName: "title", FieldId: 101,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: titles}},
		}},
	}}
}

func vectorFieldData(rows, dim int) []*schemapb.FieldData {
	vec := make([]float32, rows*dim)
	for i := range vec {
		vec[i] = float32(i) * 0.001
	}
	return []*schemapb.FieldData{{
		Type: schemapb.DataType_FloatVector, FieldName: "embedding", FieldId: 102,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: int64(dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vec}},
		}},
	}}
}

// --- Query path: RetrieveResults ---

func benchRetrieve(b *testing.B, rr *internalpb.RetrieveResults) {
	wire, err := proto.Marshal(rr)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("official", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out internalpb.RetrieveResults
			if err := proto.Unmarshal(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fastpb", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out internalpb.RetrieveResults
			if err := UnmarshalRetrieveResults(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRetrieve_Varchar(b *testing.B) {
	benchRetrieve(b, &internalpb.RetrieveResults{FieldsData: varcharFieldData(1000, 12)})
}

func BenchmarkRetrieve_Vector(b *testing.B) {
	benchRetrieve(b, &internalpb.RetrieveResults{FieldsData: vectorFieldData(1000, 768)})
}

// --- Insert path: InsertRequest (UTF-8 validated) ---

func benchInsert(b *testing.B, ir *milvuspb.InsertRequest) {
	wire, err := proto.Marshal(ir)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("official", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out milvuspb.InsertRequest
			if err := proto.Unmarshal(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fastpb", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out milvuspb.InsertRequest
			if err := UnmarshalInsertRequest(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkInsert_Varchar(b *testing.B) {
	ir := &milvuspb.InsertRequest{CollectionName: "c", FieldsData: varcharFieldData(1000, 12)}
	benchInsert(b, ir)
}

func BenchmarkInsert_Vector(b *testing.B) {
	ir := &milvuspb.InsertRequest{CollectionName: "c", FieldsData: vectorFieldData(1000, 768)}
	benchInsert(b, ir)
}

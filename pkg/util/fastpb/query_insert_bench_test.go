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

// --- Upsert path: UpsertRequest (UTF-8 validated; fields 1-8 fast, 9/10/11 fold) ---

func benchUpsert(b *testing.B, ur *milvuspb.UpsertRequest) {
	wire, err := proto.Marshal(ur)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("official", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out milvuspb.UpsertRequest
			if err := proto.Unmarshal(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fastpb", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out milvuspb.UpsertRequest
			if err := UnmarshalUpsertRequest(wire, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkUpsert_Varchar(b *testing.B) {
	benchUpsert(b, &milvuspb.UpsertRequest{CollectionName: "c", FieldsData: varcharFieldData(1000, 12)})
}

func BenchmarkUpsert_Vector(b *testing.B) {
	benchUpsert(b, &milvuspb.UpsertRequest{CollectionName: "c", FieldsData: vectorFieldData(1000, 768)})
}

// BenchmarkUpsert_ColdFields measures the fold-to-official path: the upsert-only
// fields partial_update (9) / namespace (10) / field_ops (11) are not
// hand-decoded — they accumulate into `rest` and merge via the official codec on
// top of the fast-decoded fields 1-8. This is also the shape unknown/future
// fields take.
func BenchmarkUpsert_ColdFields(b *testing.B) {
	ns := "tenant-x"
	ops := make([]*schemapb.FieldPartialUpdateOp, 16)
	for i := range ops {
		ops[i] = &schemapb.FieldPartialUpdateOp{FieldName: fmt.Sprintf("f%d", i)}
	}
	benchUpsert(b, &milvuspb.UpsertRequest{
		CollectionName: "c", NumRows: 1000, PartialUpdate: true, Namespace: &ns,
		FieldOps: ops, FieldsData: varcharFieldData(1000, 12),
	})
}

// BenchmarkUpsert_Malformed measures the reject path: a fields_data (5)
// sub-message whose declared length overruns the buffer, which both the official
// codec and fastpb must reject.
func BenchmarkUpsert_Malformed(b *testing.B) {
	wire := []byte{0x2A, 0x04, 0x08} // field 5, wtype 2, len=4, but only 1 byte follows
	b.Run("official", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out milvuspb.UpsertRequest
			_ = proto.Unmarshal(wire, &out)
		}
	})
	b.Run("fastpb", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var out milvuspb.UpsertRequest
			_ = UnmarshalUpsertRequest(wire, &out)
		}
	})
}

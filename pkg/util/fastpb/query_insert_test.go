package fastpb

import (
	"math/rand"
	"testing"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestEquiv_RetrieveResults(t *testing.T) {
	src := &internalpb.RetrieveResults{
		Base:                      &commonpb.MsgBase{MsgID: 7},
		Status:                    &commonpb.Status{Code: 0, Reason: "ok"},
		ReqID:                     99,
		Ids:                       &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
		SealedSegmentIDsRetrieved: []int64{10, 20, 30},
		ChannelIDsRetrieved:       []string{"ch1", "ch2"},
		GlobalSealedSegmentIDs:    []int64{100, 200},
		AllRetrieveCount:          3,
		HasMoreResult:             true,
		ScannedTotalBytes:         4096,
		FieldsData: []*schemapb.FieldData{
			{Type: schemapb.DataType_VarChar, FieldName: "title", FieldId: 101, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "b", "c"}}}}}},
			{Type: schemapb.DataType_FloatVector, FieldName: "emb", FieldId: 102, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}}}},
		},
	}
	wire, err := proto.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	var got internalpb.RetrieveResults
	if err := UnmarshalRetrieveResults(wire, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(src, &got) {
		t.Fatalf("mismatch:\n src=%v\n got=%v", src, &got)
	}
}

func TestEquiv_InsertRequest(t *testing.T) {
	ns := "tenant-x"
	src := &milvuspb.InsertRequest{
		Base:            &commonpb.MsgBase{MsgID: 1},
		DbName:          "db",
		CollectionName:  "coll",
		PartitionName:   "part",
		HashKeys:        []uint32{1, 2, 3, 4},
		NumRows:         3,
		SchemaTimestamp: 123456,
		Namespace:       &ns,
		FieldsData: []*schemapb.FieldData{
			{Type: schemapb.DataType_VarChar, FieldName: "title", FieldId: 101, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"x", "y", "z"}}}}}},
			{Type: schemapb.DataType_FloatVector, FieldName: "emb", FieldId: 102, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}}}},
		},
	}
	wire, err := proto.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	var got milvuspb.InsertRequest
	if err := UnmarshalInsertRequest(wire, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(src, &got) {
		t.Fatalf("mismatch:\n src=%v\n got=%v", src, &got)
	}
}

// InsertRequest is untrusted ingress: fastpb must agree with official proto on
// whether invalid UTF-8 in a string field is rejected.
func TestInsertRequest_UTF8MatchesOfficial(t *testing.T) {
	// field 3 (collection_name), wire type 2, len 2, bytes 0xFF 0xFE (invalid UTF-8)
	wire := []byte{0x1A, 0x02, 0xFF, 0xFE}
	var official milvuspb.InsertRequest
	officialErr := proto.Unmarshal(wire, &official)
	var got milvuspb.InsertRequest
	gotErr := UnmarshalInsertRequest(wire, &got)
	if (officialErr == nil) != (gotErr == nil) {
		t.Fatalf("UTF-8 accept/reject diverges: official err=%v, fastpb err=%v", officialErr, gotErr)
	}
}

func TestTryUnmarshal_Dispatch(t *testing.T) {
	// known type → handled
	rr := &internalpb.RetrieveResults{ReqID: 5}
	wire, _ := proto.Marshal(rr)
	var got internalpb.RetrieveResults
	handled, err := TryUnmarshal(&got, wire)
	if !handled || err != nil || got.ReqID != 5 {
		t.Fatalf("expected handled RetrieveResults: handled=%v err=%v reqID=%d", handled, err, got.ReqID)
	}
	// unknown type → not handled (caller falls back to official)
	var status commonpb.Status
	handled, err = TryUnmarshal(&status, nil)
	if handled || err != nil {
		t.Fatalf("expected unhandled for *commonpb.Status: handled=%v err=%v", handled, err)
	}
}

func randRetrieveResults(r *rand.Rand) *internalpb.RetrieveResults {
	rows := r.Intn(20)
	rr := &internalpb.RetrieveResults{
		ReqID:            int64(r.Uint32()),
		AllRetrieveCount: int64(r.Uint32()),
		HasMoreResult:    r.Intn(2) == 0,
	}
	if r.Intn(2) == 0 {
		ids := make([]int64, rows)
		for i := range ids {
			ids[i] = int64(r.Uint64())
		}
		rr.Ids = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
	}
	for i := 0; i < r.Intn(3); i++ {
		rr.FieldsData = append(rr.FieldsData, randFieldData(r, rows))
	}
	for i := 0; i < r.Intn(3); i++ {
		rr.ChannelIDsRetrieved = append(rr.ChannelIDsRetrieved, randString(r))
	}
	return rr
}

func randInsertRequest(r *rand.Rand) *milvuspb.InsertRequest {
	rows := r.Intn(20)
	ir := &milvuspb.InsertRequest{
		DbName:         randString(r),
		CollectionName: randString(r),
		PartitionName:  randString(r),
		NumRows:        uint32(rows),
	}
	for i := 0; i < r.Intn(3); i++ {
		ir.FieldsData = append(ir.FieldsData, randFieldData(r, rows))
	}
	hk := make([]uint32, rows)
	for i := range hk {
		hk[i] = r.Uint32()
	}
	ir.HashKeys = hk
	return ir
}

func TestEquiv_Fuzz_RetrieveResults(t *testing.T) {
	r := rand.New(rand.NewSource(0xBEEF))
	for i := 0; i < 3000; i++ {
		src := randRetrieveResults(r)
		wire, err := proto.Marshal(src)
		if err != nil {
			t.Fatalf("marshal %d: %v", i, err)
		}
		var got internalpb.RetrieveResults
		if err := UnmarshalRetrieveResults(wire, &got); err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		if !proto.Equal(src, &got) {
			t.Fatalf("mismatch %d:\n src=%v\n got=%v", i, src, &got)
		}
	}
}

func TestEquiv_Fuzz_InsertRequest(t *testing.T) {
	r := rand.New(rand.NewSource(0xFEED))
	for i := 0; i < 3000; i++ {
		src := randInsertRequest(r)
		wire, err := proto.Marshal(src)
		if err != nil {
			t.Fatalf("marshal %d: %v", i, err)
		}
		var got milvuspb.InsertRequest
		if err := UnmarshalInsertRequest(wire, &got); err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		if !proto.Equal(src, &got) {
			t.Fatalf("mismatch %d:\n src=%v\n got=%v", i, src, &got)
		}
	}
}

func TestEquiv_UpsertRequest(t *testing.T) {
	ns := "tenant-x"
	src := &milvuspb.UpsertRequest{
		Base:            &commonpb.MsgBase{MsgID: 1},
		DbName:          "db",
		CollectionName:  "coll",
		PartitionName:   "part",
		HashKeys:        []uint32{1, 2, 3, 4},
		NumRows:         3,
		SchemaTimestamp: 123456,
		PartialUpdate:   true, // field 9 (varint)  → folded to official merge
		Namespace:       &ns,  // field 10 (bytes)  → folded to official merge
		FieldOps: []*schemapb.FieldPartialUpdateOp{ // field 11 (message) → folded
			{FieldName: "title"},
		},
		FieldsData: []*schemapb.FieldData{
			{Type: schemapb.DataType_VarChar, FieldName: "title", FieldId: 101, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"x", "y", "z"}}}}}},
			{Type: schemapb.DataType_FloatVector, FieldName: "emb", FieldId: 102, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6}}}}}},
		},
	}
	wire, err := proto.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	var got milvuspb.UpsertRequest
	if err := UnmarshalUpsertRequest(wire, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(src, &got) {
		t.Fatalf("mismatch:\n src=%v\n got=%v", src, &got)
	}
	// TryUnmarshal must dispatch UpsertRequest to fastpb (RPC-boundary coverage).
	var viaDispatch milvuspb.UpsertRequest
	handled, err := TryUnmarshal(&viaDispatch, wire)
	if !handled || err != nil || !proto.Equal(src, &viaDispatch) {
		t.Fatalf("TryUnmarshal(UpsertRequest) dispatch failed: handled=%v err=%v", handled, err)
	}
}

// UpsertRequest is untrusted ingress too: fastpb must agree with official proto on
// whether invalid UTF-8 in a string field is rejected.
func TestUpsertRequest_UTF8MatchesOfficial(t *testing.T) {
	// field 3 (collection_name), wire type 2, len 2, bytes 0xFF 0xFE (invalid UTF-8)
	wire := []byte{0x1A, 0x02, 0xFF, 0xFE}
	var official milvuspb.UpsertRequest
	officialErr := proto.Unmarshal(wire, &official)
	var got milvuspb.UpsertRequest
	gotErr := UnmarshalUpsertRequest(wire, &got)
	if (officialErr == nil) != (gotErr == nil) {
		t.Fatalf("UTF-8 accept/reject diverges: official err=%v, fastpb err=%v", officialErr, gotErr)
	}
}

func randUpsertRequest(r *rand.Rand) *milvuspb.UpsertRequest {
	rows := r.Intn(20)
	ur := &milvuspb.UpsertRequest{
		DbName:         randString(r),
		CollectionName: randString(r),
		PartitionName:  randString(r),
		NumRows:        uint32(rows),
		PartialUpdate:  r.Intn(2) == 0, // exercise the field-9 fold path
	}
	for i := 0; i < r.Intn(3); i++ {
		ur.FieldsData = append(ur.FieldsData, randFieldData(r, rows))
	}
	hk := make([]uint32, rows)
	for i := range hk {
		hk[i] = r.Uint32()
	}
	ur.HashKeys = hk
	if r.Intn(2) == 0 {
		ns := randString(r) // exercise the field-10 fold path
		ur.Namespace = &ns
	}
	for i := 0; i < r.Intn(3); i++ { // exercise the field-11 (field_ops) fold path
		ur.FieldOps = append(ur.FieldOps, &schemapb.FieldPartialUpdateOp{
			FieldName: randString(r),
			Op:        schemapb.FieldPartialUpdateOp_OpType(r.Intn(2)),
		})
	}
	return ur
}

func TestEquiv_Fuzz_UpsertRequest(t *testing.T) {
	r := rand.New(rand.NewSource(0xCAFE))
	for i := 0; i < 3000; i++ {
		src := randUpsertRequest(r)
		wire, err := proto.Marshal(src)
		if err != nil {
			t.Fatalf("marshal %d: %v", i, err)
		}
		var got milvuspb.UpsertRequest
		if err := UnmarshalUpsertRequest(wire, &got); err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		if !proto.Equal(src, &got) {
			t.Fatalf("mismatch %d:\n src=%v\n got=%v", i, src, &got)
		}
	}
}

package fastpb

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	msgpb "github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func fieldDescriptorContract(m proto.Message) string {
	fds := m.ProtoReflect().Descriptor().Fields()
	type contract struct {
		number protoreflect.FieldNumber
		text   string
	}
	contracts := make([]contract, 0, fds.Len())
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		typeName := "-"
		if fd.Message() != nil {
			typeName = string(fd.Message().FullName())
		} else if fd.Enum() != nil {
			typeName = string(fd.Enum().FullName())
		}
		oneofName := "-"
		oneofSynthetic := false
		if oneof := fd.ContainingOneof(); oneof != nil {
			oneofName = string(oneof.FullName())
			oneofSynthetic = oneof.IsSynthetic()
		}
		contracts = append(contracts, contract{
			number: fd.Number(),
			text: fmt.Sprintf("%s#%d/%s/%s/packed=%t/type=%s/oneof=%s/synthetic=%t/presence=%t",
				fd.Name(), fd.Number(), fd.Kind(), fd.Cardinality(), fd.IsPacked(), typeName, oneofName, oneofSynthetic, fd.HasPresence()),
		})
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].number < contracts[j].number
	})
	parts := make([]string, len(contracts))
	for i, contract := range contracts {
		parts[i] = contract.text
	}
	return strings.Join(parts, ";")
}

// TestProtoContract_FieldDescriptorsPinned is a tripwire. fastpb hand-writes
// wire codecs for the message types below; each codec depends on exact field
// names, numbers, kinds, cardinality, packed representation, message/enum type,
// presence, and oneof membership. A stable digest of that complete normalized
// descriptor tuple is pinned for every type.
//
// When it fails, do NOT just bump the digest. First go read the matching
// hand-written codec in pkg/util/fastpb and decide what the proto change requires:
//
//   - A new plain (non-oneof) scalar/message field may be correct-by-construction
//     for a decoder that folds unknown fields through `rest -> protoMerge`.
//     Encoders do not have that fallback: decide whether the field belongs to
//     output metadata, selected row data, or must be rejected explicitly.
//
//   - A NEW oneof VARIANT is the dangerous case. It also falls through to the
//     deferred protoMerge, but that breaks oneof last-wins ordering relative to
//     the variants decoded in-pass (see the StructArrays/case-8 comment in
//     fielddata.go). New oneof variants on FieldData / ScalarField / VectorField
//     / IDs MUST be handled in-pass, not left to the fallback.
//
//   - Changing the wire type or meaning of an EXISTING hard-coded number
//     silently corrupts the fast path — the hard-coded case still fires.
//
// Only after every affected codec is correct should you update the digest.
func TestProtoContract_FieldDescriptorsPinned(t *testing.T) {
	cases := []struct {
		name string
		msg  proto.Message
		want string
	}{
		// top-level fast-pathed types (TryUnmarshal dispatch)
		{"internalpb.RetrieveResults", &internalpb.RetrieveResults{}, "2b1e7ae8a633e6145c1f352e2653f28d561d144629be05fc7b18ccf7022518a6"},
		{"milvuspb.InsertRequest", &milvuspb.InsertRequest{}, "a334da7b112573d379549d8c81c1afcf0a8526488947c4868b6ff4d9413b29a7"},
		{"milvuspb.UpsertRequest", &milvuspb.UpsertRequest{}, "a0fc2a36ce1d4f67f0d73a4c7faabbf0d7c76b522ce8acc851c149f311c78d50"},
		{"msgpb.InsertRequest", &msgpb.InsertRequest{}, "c98eeac7deeace0770cdafbb910aec8fb2a1de9e152f99821bb260afdd19c44d"},
		{"schemapb.SearchResultData", &schemapb.SearchResultData{}, "f17c51046b4baf17f67a48938a244aaa0a5ca50fbfe649fd0e88e03af8c53ecd"},
		// nested hand-decoded types (oneof-bearing — highest divergence risk)
		{"schemapb.FieldData", &schemapb.FieldData{}, "dde2cc5e2e91394c5f3c2099f2529b7ef2e6f6707744c98a73bd4b86505cc081"},
		{"schemapb.ScalarField", &schemapb.ScalarField{}, "4e8d38191186856584bf0b02593b8d6f513b775845b1c1cebddeb98cff11ffda"},
		{"schemapb.VectorField", &schemapb.VectorField{}, "157f02036ff1af36827de849900cb9a0430e99258acded8e11a589b227df0411"},
		{"schemapb.IDs", &schemapb.IDs{}, "350a0a056fcd16e1aa576276108dcfac0ff2dd368d51bb49a7c9e05b67e00a6c"},
		// leaf arrays with their own hand-written field switches (no unknown-field
		// fallback in some — a new field here would mis-parse)
		{"schemapb.SparseFloatArray", &schemapb.SparseFloatArray{}, "7f2a00a40e7a3e7664403c3d08d7753e1d3044ff074db735a6b20dc98737b813"},
		{"schemapb.FloatArray", &schemapb.FloatArray{}, "7338e15ebca10fe312d1e9820dc09fdc49fec68905b162966271e9f58313efde"},
		{"schemapb.LongArray", &schemapb.LongArray{}, "3aea90038f9e62d79ad96e6524f939d9130a06539ce54c78eda45a11e3028291"},
		{"schemapb.IntArray", &schemapb.IntArray{}, "7c1ee638141b8de8c9d740fcb70bbc7c76d38817cb6d5380b3e8f7690f2f8370"},
		{"schemapb.BoolArray", &schemapb.BoolArray{}, "e03d41a1a7eb0a5d1d17a7455df71cca7474e4b0d55eac0df979d9cfabeca0c7"},
		{"schemapb.DoubleArray", &schemapb.DoubleArray{}, "65647c549e381e12de24c66049c31f9fefce216e0af82457829bd8e7a6fdee9d"},
		{"schemapb.BytesArray", &schemapb.BytesArray{}, "bbc4d3675300c40250e658c62d35489560260a781142b03d330e5bf6618baeab"},
		{"schemapb.ArrayArray", &schemapb.ArrayArray{}, "03f99b445863ce638c0132bbfdfd15db193f9adefca807e5ba710129f1ce9d64"},
		{"schemapb.JSONArray", &schemapb.JSONArray{}, "bbc4d3675300c40250e658c62d35489560260a781142b03d330e5bf6618baeab"},
		{"schemapb.UUIDArray", &schemapb.UUIDArray{}, "bbc4d3675300c40250e658c62d35489560260a781142b03d330e5bf6618baeab"},
		{"schemapb.GeometryArray", &schemapb.GeometryArray{}, "bbc4d3675300c40250e658c62d35489560260a781142b03d330e5bf6618baeab"},
		{"schemapb.TimestamptzArray", &schemapb.TimestamptzArray{}, "3aea90038f9e62d79ad96e6524f939d9130a06539ce54c78eda45a11e3028291"},
		{"schemapb.GeometryWktArray", &schemapb.GeometryWktArray{}, "80b5e36020cb25d39d76417531402eaba350456c82050dfa96654473dc59f298"},
		{"schemapb.MolArray", &schemapb.MolArray{}, "bbc4d3675300c40250e658c62d35489560260a781142b03d330e5bf6618baeab"},
		{"schemapb.MolSmilesArray", &schemapb.MolSmilesArray{}, "80b5e36020cb25d39d76417531402eaba350456c82050dfa96654473dc59f298"},
		{"schemapb.DateArray", &schemapb.DateArray{}, "7c1ee638141b8de8c9d740fcb70bbc7c76d38817cb6d5380b3e8f7690f2f8370"},
		{"schemapb.TimeArray", &schemapb.TimeArray{}, "3aea90038f9e62d79ad96e6524f939d9130a06539ce54c78eda45a11e3028291"},
		{"schemapb.StringArray", &schemapb.StringArray{}, "80b5e36020cb25d39d76417531402eaba350456c82050dfa96654473dc59f298"},
		{"schemapb.VectorArray", &schemapb.VectorArray{}, "4a2324ee4ff40a0723da8b011125c7762e2739c3de63e4dde7e6025faa532e17"},
	}
	for _, c := range cases {
		contract := fieldDescriptorContract(c.msg)
		got := fmt.Sprintf("%x", sha256.Sum256([]byte(contract)))
		assert.Equalf(t, c.want, got,
			"%s proto field descriptor changed (want digest %s, got %s).\n"+
				"Normalized descriptor: %s\n"+
				"This type has a hand-written codec in pkg/util/fastpb. Read the codec and the\n"+
				"guidance on TestProtoContract_FieldDescriptorsPinned BEFORE updating this digest —\n"+
				"a new oneof variant in particular requires explicit handling.",
			c.name, c.want, got, contract)
	}
}

// TestInsertRequestViewEncoder_FieldNumbersCovered makes the encoder's field
// ownership explicit. The descriptor digest above catches every proto change,
// but this independent allowlist prevents a future contributor from merely
// updating that digest after adding a field that appendRequest does not emit.
//
// row_data is the only intentional omission: the Proxy uses the column-based
// representation and materializeWithAppendFieldData drops this compatibility
// field as well. Every other known field is emitted from either the metadata
// template or the selected row view.
func TestInsertRequestViewEncoder_FieldNumbersCovered(t *testing.T) {
	type fieldContract struct {
		name    protoreflect.Name
		emitted bool
	}
	contracts := map[protoreflect.FieldNumber]fieldContract{
		1:  {name: "base", emitted: true},
		2:  {name: "shardName", emitted: true},
		3:  {name: "db_name", emitted: true},
		4:  {name: "collection_name", emitted: true},
		5:  {name: "partition_name", emitted: true},
		6:  {name: "dbID", emitted: true},
		7:  {name: "collectionID", emitted: true},
		8:  {name: "partitionID", emitted: true},
		9:  {name: "segmentID", emitted: true},
		10: {name: "timestamps", emitted: true},
		11: {name: "rowIDs", emitted: true},
		12: {name: "row_data", emitted: false},
		13: {name: "fields_data", emitted: true},
		14: {name: "num_rows", emitted: true},
		15: {name: "version", emitted: true},
		16: {name: "namespace", emitted: true},
	}

	fields := (&msgpb.InsertRequest{}).ProtoReflect().Descriptor().Fields()
	assert.Equal(t, len(contracts), fields.Len(),
		"msgpb.InsertRequest gained or lost a field; classify it in the view encoder contract")
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		contract, ok := contracts[field.Number()]
		if !assert.Truef(t, ok,
			"msgpb.InsertRequest field %s (#%d) is not classified by InsertRequestViewEncoder",
			field.FullName(), field.Number()) {
			continue
		}
		assert.Equalf(t, contract.name, field.Name(),
			"msgpb.InsertRequest field #%d changed identity; audit InsertRequestViewEncoder before updating the contract",
			field.Number())
		if !contract.emitted {
			assert.Equal(t, protoreflect.FieldNumber(12), field.Number(),
				"only the legacy row_data field may be intentionally omitted")
		}
	}
}

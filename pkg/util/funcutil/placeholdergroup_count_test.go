package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// countByUnmarshal is what CountPlaceholderGroupValues must agree with.
func countByUnmarshal(blob []byte) (int64, error) {
	group := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(blob, group); err != nil {
		return 0, err
	}
	var total int64
	for _, placeholder := range group.GetPlaceholders() {
		total += int64(len(placeholder.GetValues()))
	}
	return total, nil
}

func marshalGroup(t testing.TB, placeholders ...*commonpb.PlaceholderValue) []byte {
	blob, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: placeholders})
	require.NoError(t, err)
	return blob
}

func rows(n, size int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = make([]byte, size)
		for j := range out[i] {
			out[i][j] = byte(i + j)
		}
	}
	return out
}

func placeholderGroupSeeds(t testing.TB) map[string][]byte {
	// Unknown fields, a group-typed field, the known field numbers with the
	// wrong wire type and a repeated type field: all legal on the wire, and
	// none of them is a row.
	var handBuilt []byte
	handBuilt = protowire.AppendTag(handBuilt, 9, protowire.VarintType)
	handBuilt = protowire.AppendVarint(handBuilt, 7)
	var value []byte
	value = protowire.AppendTag(value, 3, protowire.VarintType)
	value = protowire.AppendVarint(value, 1)
	value = protowire.AppendTag(value, 2, protowire.VarintType)
	value = protowire.AppendVarint(value, uint64(commonpb.PlaceholderType_BinaryVector))
	value = protowire.AppendTag(value, 12, protowire.StartGroupType)
	value = protowire.AppendTag(value, 1, protowire.BytesType)
	value = protowire.AppendBytes(value, []byte("in a group"))
	value = protowire.AppendTag(value, 12, protowire.EndGroupType)
	value = protowire.AppendTag(value, 3, protowire.BytesType)
	value = protowire.AppendBytes(value, []byte{1, 2, 3, 4})
	value = protowire.AppendTag(value, 2, protowire.VarintType)
	value = protowire.AppendVarint(value, uint64(commonpb.PlaceholderType_FloatVector))
	value = protowire.AppendTag(value, 3, protowire.BytesType)
	value = protowire.AppendBytes(value, []byte{5, 6, 7, 8})
	handBuilt = protowire.AppendTag(handBuilt, 1, protowire.BytesType)
	handBuilt = protowire.AppendBytes(handBuilt, value)
	handBuilt = protowire.AppendTag(handBuilt, 1, protowire.Fixed64Type)
	handBuilt = protowire.AppendFixed64(handBuilt, 42)

	return map[string][]byte{
		"empty input":       {},
		"empty placeholder": marshalGroup(t, &commonpb.PlaceholderValue{Tag: "$0"}),
		"one float vector": marshalGroup(t, &commonpb.PlaceholderValue{
			Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: rows(1, 16),
		}),
		"many float vectors": marshalGroup(t, &commonpb.PlaceholderValue{
			Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: rows(300, 32),
		}),
		"sparse rows of different length": marshalGroup(t, &commonpb.PlaceholderValue{
			Tag: "$0", Type: commonpb.PlaceholderType_SparseFloatVector,
			Values: [][]byte{make([]byte, 8), {}, make([]byte, 64), make([]byte, 16)},
		}),
		"embedding lists": marshalGroup(t, &commonpb.PlaceholderValue{
			Tag: "$0", Type: commonpb.PlaceholderType_EmbListFloatVector, Values: rows(3, 48), ElementLevel: true,
		}),
		"text": marshalGroup(t, &commonpb.PlaceholderValue{
			Tag: "$0", Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{[]byte("hello"), []byte("向量检索")},
		}),
		"two placeholders": marshalGroup(t,
			&commonpb.PlaceholderValue{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: rows(2, 8)},
			&commonpb.PlaceholderValue{Tag: "$1", Type: commonpb.PlaceholderType_BinaryVector, Values: rows(5, 4)},
		),
		"unknown fields and wrong wire types": handBuilt,
		// A field number above the protobuf limit, and a tag that is not
		// valid UTF-8: both found by FuzzCountPlaceholderGroupValues as inputs
		// proto.Unmarshal refuses.
		"field number out of range": {0xa2, 0xa2, 0xa2, 0xa2, 0x30, 0x01, 0x30},
		"tag is not valid utf8":     {0x0a, 0x03, 0x0a, 0x01, 0xff},
	}
}

// requireSameAsUnmarshal is the whole contract: a group proto.Unmarshal accepts
// is accepted with the same count, and one it refuses is refused.
func requireSameAsUnmarshal(t testing.TB, blob []byte) {
	want, wantErr := countByUnmarshal(blob)
	got, gotErr := CountPlaceholderGroupValues(blob)
	if wantErr != nil {
		require.Error(t, gotErr, "proto.Unmarshal refuses %x: %v", blob, wantErr)
		return
	}
	require.NoError(t, gotErr, "proto.Unmarshal accepts %x", blob)
	require.Equal(t, want, got, "input %x", blob)
}

func TestCountPlaceholderGroupValues(t *testing.T) {
	for name, blob := range placeholderGroupSeeds(t) {
		t.Run(name, func(t *testing.T) {
			requireSameAsUnmarshal(t, blob)
		})
	}

	t.Run("counts", func(t *testing.T) {
		seeds := placeholderGroupSeeds(t)
		for name, want := range map[string]int64{
			"empty input":                         0,
			"empty placeholder":                   0,
			"many float vectors":                  300,
			"sparse rows of different length":     4,
			"two placeholders":                    7,
			"unknown fields and wrong wire types": 2,
		} {
			got, err := CountPlaceholderGroupValues(seeds[name])
			assert.NoError(t, err, name)
			assert.Equal(t, want, got, name)
		}
	})

	t.Run("every truncation", func(t *testing.T) {
		blob := placeholderGroupSeeds(t)["two placeholders"]
		for cut := 0; cut <= len(blob); cut++ {
			requireSameAsUnmarshal(t, blob[:cut])
		}
	})

	t.Run("does not allocate", func(t *testing.T) {
		blob := marshalGroup(t, &commonpb.PlaceholderValue{
			Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: rows(1000, 3072),
		})
		assert.Zero(t, testing.AllocsPerRun(10, func() {
			_, _ = CountPlaceholderGroupValues(blob)
		}))
	})
}

func FuzzCountPlaceholderGroupValues(f *testing.F) {
	for _, blob := range placeholderGroupSeeds(f) {
		f.Add(blob)
	}
	f.Fuzz(func(t *testing.T, blob []byte) {
		requireSameAsUnmarshal(t, blob)
	})
}

package fastpb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Date/time are message-valued oneof variants added after the original decoder.
// They must be consumed in wire order, rather than replayed in a deferred merge.
func TestScalarDateTimeWireOrderAndMerge(t *testing.T) {
	variants := []*schemapb.ScalarField{
		{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"hot"}}}},
		{Data: &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: []int32{2, -3}}}},
		{Data: &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{Data: []int64{4, -5}}}},
	}
	for i, first := range variants {
		for j, last := range variants {
			t.Run(fmt.Sprintf("%d_then_%d", i, j), func(t *testing.T) {
				wire := concat(t, first, last)
				want, got := &schemapb.ScalarField{}, &schemapb.ScalarField{}
				require.NoError(t, proto.Unmarshal(wire, want))
				require.NoError(t, (dec{}).scalarField(wire, got))
				require.True(t, proto.Equal(want, got), "got=%v want=%v", got, want)
				for k := range wire {
					wire[k] = 0
				}
				require.True(t, proto.Equal(want, got))
			})
		}
	}
	for _, num := range []protowire.Number{15, 16} {
		t.Run(fmt.Sprintf("wrong_wire_%d", num), func(t *testing.T) {
			wire := protowire.AppendTag(nil, num, protowire.VarintType)
			wire = protowire.AppendVarint(wire, 42)
			want, got := &schemapb.ScalarField{}, &schemapb.ScalarField{}
			require.NoError(t, proto.Unmarshal(wire, want))
			require.NoError(t, (dec{}).scalarField(wire, got))
			require.True(t, proto.Equal(want, got))
		})
	}
}

// RetrieveResults field 20 is an ordinary scalar, preserved by official merge.
// Explicitly encoded defaults still obey last-wins behavior.
func TestRetrieveResultsMVCCTimestampFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []uint64
		want   uint64
	}{
		{"nonzero", []uint64{123}, 123},
		{"default_then_nonzero", []uint64{0, 123}, 123},
		{"nonzero_then_default", []uint64{123, 0}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire []byte
			for _, value := range tc.values {
				wire = protowire.AppendTag(wire, 20, protowire.VarintType)
				wire = protowire.AppendVarint(wire, value)
			}
			want, got := &internalpb.RetrieveResults{}, &internalpb.RetrieveResults{}
			require.NoError(t, proto.Unmarshal(wire, want))
			require.NoError(t, UnmarshalRetrieveResults(wire, got))
			require.True(t, proto.Equal(want, got))
			require.Equal(t, tc.want, got.MvccTimestamp)
		})
	}
}

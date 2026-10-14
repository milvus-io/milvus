package shallowcopy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestShallowCopySearchRequest(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		assert.Nil(t, ShallowCopySearchRequest(nil, 1))
	})

	t.Run("copies all fields and sets targetID", func(t *testing.T) {
		src := &internalpb.SearchRequest{
			Base:                    &commonpb.MsgBase{TargetID: 100, SourceID: 200},
			ReqID:                   1,
			DbID:                    2,
			CollectionID:            3,
			PartitionIDs:            []int64{10, 20},
			Dsl:                     "dsl",
			PlaceholderGroup:        []byte("placeholder"),
			SerializedExprPlan:      []byte("plan"),
			OutputFieldsId:          []int64{1, 2},
			MvccTimestamp:           100,
			Nq:                      5,
			Topk:                    10,
			MetricType:              "L2",
			IgnoreGrowing:           true,
			Username:                "user",
			IsAdvanced:              true,
			Offset:                  5,
			GroupByFieldId:          7,
			GroupSize:               3,
			FieldId:                 8,
			IsTopkReduce:            true,
			IsRecallEvaluation:      true,
			IsIterator:              true,
			AnalyzerName:            "analyzer",
			CollectionTtlTimestamps: 999,
		}

		dst := ShallowCopySearchRequest(src, 42)

		// Base is new with correct TargetID
		assert.Equal(t, int64(42), dst.Base.TargetID)
		// Original Base not modified
		assert.Equal(t, int64(100), src.Base.TargetID)

		// Scalar fields copied
		assert.Equal(t, src.ReqID, dst.ReqID)
		assert.Equal(t, src.CollectionID, dst.CollectionID)
		assert.Equal(t, src.Nq, dst.Nq)
		assert.Equal(t, src.Topk, dst.Topk)
		assert.Equal(t, src.IsIterator, dst.IsIterator)
		assert.Equal(t, src.AnalyzerName, dst.AnalyzerName)

		// Slices share underlying array (shallow copy)
		assert.Equal(t, src.PartitionIDs, dst.PartitionIDs)
		assert.Equal(t, src.PlaceholderGroup, dst.PlaceholderGroup)
		assert.Equal(t, src.SerializedExprPlan, dst.SerializedExprPlan)
	})
}

func TestShallowCopyRetrieveRequest(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		assert.Nil(t, ShallowCopyRetrieveRequest(nil, 1))
	})

	t.Run("copies all fields and sets targetID", func(t *testing.T) {
		src := &internalpb.RetrieveRequest{
			Base:                    &commonpb.MsgBase{TargetID: 100},
			ReqID:                   1,
			CollectionID:            3,
			PartitionIDs:            []int64{10, 20},
			SerializedExprPlan:      []byte("plan"),
			OutputFieldsId:          []int64{1, 2},
			MvccTimestamp:           100,
			Limit:                   50,
			IgnoreGrowing:           true,
			IsCount:                 true,
			Username:                "user",
			IsIterator:              true,
			CollectionTtlTimestamps: 999,
			QueryLabel:              "query",
		}

		dst := ShallowCopyRetrieveRequest(src, 42)

		assert.Equal(t, int64(42), dst.Base.TargetID)
		assert.Equal(t, int64(100), src.Base.TargetID)
		assert.Equal(t, src.ReqID, dst.ReqID)
		assert.Equal(t, src.CollectionID, dst.CollectionID)
		assert.Equal(t, src.Limit, dst.Limit)
		assert.Equal(t, src.IsIterator, dst.IsIterator)
		assert.Equal(t, src.QueryLabel, dst.QueryLabel)
		assert.Equal(t, src.PartitionIDs, dst.PartitionIDs)
	})
}

// populateAllFields sets every field of m to a non-zero value, so that a field
// missing from a shallow copy shows up as unset on the copy.
func populateAllFields(t *testing.T, m protoreflect.Message) {
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch {
		case fd.IsMap():
			t.Fatalf("map field %s is not handled by this test", fd.Name())
		case fd.IsList():
			list := m.Mutable(fd).List()
			if fd.Message() != nil {
				list.Append(list.NewElement())
			} else {
				list.Append(nonZeroScalar(t, fd))
			}
		case fd.Message() != nil:
			m.Set(fd, protoreflect.ValueOfMessage(m.NewField(fd).Message()))
		default:
			m.Set(fd, nonZeroScalar(t, fd))
		}
	}
}

func nonZeroScalar(t *testing.T, fd protoreflect.FieldDescriptor) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(1)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(7)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(7)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(7)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(7)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("x")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{1})
	default:
		t.Fatalf("field %s has unhandled kind %s", fd.Name(), fd.Kind())
		return protoreflect.Value{}
	}
}

// assertAllFieldsCopied fails when a field of the source message is not carried over
// to the copy. The copy functions list fields by hand, so a field added to the proto
// later would otherwise be dropped silently on the way to the query nodes.
func assertAllFieldsCopied(t *testing.T, src, dst protoreflect.Message) {
	fields := src.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Name() == "base" {
			continue
		}
		assert.Truef(t, src.Has(fd), "test setup did not populate field %s", fd.Name())
		assert.Truef(t, dst.Has(fd), "field %s is not copied by the shallow copy", fd.Name())
	}
}

func TestShallowCopyCoversAllFields(t *testing.T) {
	t.Run("SearchRequest", func(t *testing.T) {
		src := &internalpb.SearchRequest{}
		populateAllFields(t, src.ProtoReflect())
		dst := ShallowCopySearchRequest(src, 42)
		assertAllFieldsCopied(t, src.ProtoReflect(), dst.ProtoReflect())
		assert.Equal(t, int64(42), dst.GetBase().GetTargetID())
	})

	t.Run("RetrieveRequest", func(t *testing.T) {
		src := &internalpb.RetrieveRequest{}
		populateAllFields(t, src.ProtoReflect())
		dst := ShallowCopyRetrieveRequest(src, 42)
		assertAllFieldsCopied(t, src.ProtoReflect(), dst.ProtoReflect())
		assert.Equal(t, int64(42), dst.GetBase().GetTargetID())
	})
}

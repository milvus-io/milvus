package shallowcopy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func populatedScalarValue(field protoreflect.FieldDescriptor) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(1)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("value")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte("value"))
	default:
		panic("not a scalar protobuf field")
	}
}

// populateAllProtoFields makes proto.Equal catch any field omitted by a manual
// shallow-copy implementation, including fields added to the proto later.
func populateAllProtoFields(message proto.Message) {
	reflection := message.ProtoReflect()
	fields := reflection.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		switch {
		case field.IsMap():
			values := reflection.Mutable(field).Map()
			key := populatedScalarValue(field.MapKey()).MapKey()
			if field.MapValue().Kind() == protoreflect.MessageKind {
				values.Set(key, values.NewValue())
			} else {
				values.Set(key, populatedScalarValue(field.MapValue()))
			}
		case field.IsList():
			values := reflection.Mutable(field).List()
			if field.Kind() == protoreflect.MessageKind {
				values.Append(values.NewElement())
			} else {
				values.Append(populatedScalarValue(field))
			}
		case field.Kind() == protoreflect.MessageKind:
			reflection.Mutable(field)
		default:
			reflection.Set(field, populatedScalarValue(field))
		}
	}
}

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
			GroupByFieldIds:         []int64{7, 9},
			GroupSize:               3,
			FieldId:                 8,
			IsTopkReduce:            true,
			IsRecallEvaluation:      true,
			IsIterator:              true,
			AnalyzerName:            "analyzer",
			CollectionTtlTimestamps: 999,
			EntityTtlPhysicalTime:   888,
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
		assert.Equal(t, src.EntityTtlPhysicalTime, dst.EntityTtlPhysicalTime)

		// Slices share underlying array (shallow copy)
		assert.Equal(t, src.PartitionIDs, dst.PartitionIDs)
		assert.Equal(t, src.PlaceholderGroup, dst.PlaceholderGroup)
		assert.Equal(t, src.SerializedExprPlan, dst.SerializedExprPlan)
		assert.Equal(t, src.GroupByFieldIds, dst.GroupByFieldIds)
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
			EntityTtlPhysicalTime:   888,
			QueryLabel:              "query",
		}

		dst := ShallowCopyRetrieveRequest(src, 42)

		assert.Equal(t, int64(42), dst.Base.TargetID)
		assert.Equal(t, int64(100), src.Base.TargetID)
		assert.Equal(t, src.ReqID, dst.ReqID)
		assert.Equal(t, src.CollectionID, dst.CollectionID)
		assert.Equal(t, src.Limit, dst.Limit)
		assert.Equal(t, src.IsIterator, dst.IsIterator)
		assert.Equal(t, src.EntityTtlPhysicalTime, dst.EntityTtlPhysicalTime)
		assert.Equal(t, src.QueryLabel, dst.QueryLabel)
		assert.Equal(t, src.PartitionIDs, dst.PartitionIDs)
	})
}

func TestShallowCopyLoadSegmentsRequest(t *testing.T) {
	assert.Nil(t, ShallowCopyLoadSegmentsRequest(nil))

	src := &querypb.LoadSegmentsRequest{}
	populateAllProtoFields(src)
	dst := ShallowCopyLoadSegmentsRequest(src)

	require.True(t, proto.Equal(src, dst))
	assert.NotSame(t, src, dst)
	assert.Same(t, src.GetBase(), dst.GetBase())
	assert.Same(t, src.GetInfos()[0], dst.GetInfos()[0])
	assert.Same(t, src.GetSchema(), dst.GetSchema())
	assert.Same(t, src.GetIndexInfoList()[0], dst.GetIndexInfoList()[0])
}

func TestShallowCopySegmentLoadInfo(t *testing.T) {
	assert.Nil(t, ShallowCopySegmentLoadInfo(nil))

	src := &querypb.SegmentLoadInfo{}
	populateAllProtoFields(src)
	dst := ShallowCopySegmentLoadInfo(src)

	require.True(t, proto.Equal(src, dst))
	assert.NotSame(t, src, dst)
	assert.Same(t, src.GetBinlogPaths()[0], dst.GetBinlogPaths()[0])
	assert.Same(t, src.GetIndexInfos()[0], dst.GetIndexInfos()[0])
	assert.Same(t, src.GetStats(), dst.GetStats())
}

func TestShallowCopyFieldIndexInfo(t *testing.T) {
	assert.Nil(t, ShallowCopyFieldIndexInfo(nil))

	src := &querypb.FieldIndexInfo{}
	populateAllProtoFields(src)
	dst := ShallowCopyFieldIndexInfo(src)

	require.True(t, proto.Equal(src, dst))
	assert.NotSame(t, src, dst)
	assert.Same(t, src.GetIndexParams()[0], dst.GetIndexParams()[0])
	assert.True(t, &src.IndexFilePaths[0] == &dst.IndexFilePaths[0])
}

package shallowcopy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
)

func TestShallowCopyRequestBase(t *testing.T) {
	type request interface {
		proto.Message
		GetBase() *commonpb.MsgBase
	}
	for _, tc := range []struct {
		name    string
		msgType commonpb.MsgType
		newReq  func(*commonpb.MsgBase) request
		copyReq func(request, int64) request
	}{
		{
			name: "search", msgType: commonpb.MsgType_Search,
			newReq: func(base *commonpb.MsgBase) request {
				return &internalpb.SearchRequest{Base: base, PlaceholderGroup: []byte("placeholder")}
			},
			copyReq: func(src request, targetID int64) request {
				return ShallowCopySearchRequest(src.(*internalpb.SearchRequest), targetID)
			},
		},
		{
			name: "retrieve", msgType: commonpb.MsgType_Retrieve,
			newReq: func(base *commonpb.MsgBase) request {
				return &internalpb.RetrieveRequest{Base: base, SerializedExprPlan: []byte("plan")}
			},
			copyReq: func(src request, targetID int64) request {
				return ShallowCopyRetrieveRequest(src.(*internalpb.RetrieveRequest), targetID)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Proxy creates the base, then the scheduler assigns its timestamp and ID.
			base := commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(tc.msgType),
				commonpbutil.WithSourceID(200),
			)
			base.Timestamp = 12345
			base.MsgID = 12345
			base.Properties = map[string]string{"key": "value"}
			src := tc.newReq(base)
			original := proto.Clone(src)

			// Both proxy -> delegator and delegator -> worker use the same helper.
			leader := tc.copyReq(src, 42)
			worker := tc.copyReq(leader, 43)
			sibling := tc.copyReq(leader, 44)
			for _, dst := range []request{leader, worker, sibling} {
				assert.Equal(t, base.MsgID, dst.GetBase().GetMsgID(), "forwarding must preserve the proxy message ID")
				expected := proto.Clone(base).(*commonpb.MsgBase)
				expected.TargetID = dst.GetBase().GetTargetID()
				assert.True(t, proto.Equal(expected, dst.GetBase()), "forwarding must preserve base metadata")
				require.NotSame(t, base, dst.GetBase())
			}
			assert.Equal(t, int64(42), leader.GetBase().GetTargetID())
			assert.Equal(t, int64(43), worker.GetBase().GetTargetID())
			assert.Equal(t, int64(44), sibling.GetBase().GetTargetID())
			require.NotSame(t, leader.GetBase(), worker.GetBase())
			require.NotSame(t, worker.GetBase(), sibling.GetBase())

			// Remote forwarding serializes the copied request; local workers use it directly.
			data, err := proto.Marshal(worker)
			require.NoError(t, err)
			decoded := tc.newReq(nil)
			require.NoError(t, proto.Unmarshal(data, decoded))
			assert.True(t, proto.Equal(worker, decoded))
			assert.Equal(t, base.MsgID, decoded.GetBase().GetMsgID())

			worker.GetBase().MsgID = 99
			if worker.GetBase().Properties != nil {
				worker.GetBase().Properties["key"] = "changed"
			}
			assert.True(t, proto.Equal(original, src), "forwarding must not mutate the source")
			assert.Equal(t, base.MsgID, leader.GetBase().GetMsgID())
			assert.Equal(t, "value", sibling.GetBase().GetProperties()["key"])

			for _, emptyBase := range []*commonpb.MsgBase{nil, {}} {
				dst := tc.copyReq(tc.newReq(emptyBase), 45)
				require.NotNil(t, dst.GetBase())
				assert.True(t, proto.Equal(&commonpb.MsgBase{TargetID: 45}, dst.GetBase()))
			}
		})
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
			SchemaVersion:           7,
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
		assert.Equal(t, src.SchemaVersion, dst.SchemaVersion)
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
			SchemaVersion:           7,
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
		assert.Equal(t, src.SchemaVersion, dst.SchemaVersion)
		assert.Equal(t, src.Limit, dst.Limit)
		assert.Equal(t, src.IsIterator, dst.IsIterator)
		assert.Equal(t, src.EntityTtlPhysicalTime, dst.EntityTtlPhysicalTime)
		assert.Equal(t, src.QueryLabel, dst.QueryLabel)
		assert.Equal(t, src.PartitionIDs, dst.PartitionIDs)
	})
}

package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadCollectionWarmupHintValidation(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name    string
		params  map[string]string
		refresh bool
		valid   bool
	}{
		{"omitted", nil, false, true},
		{"true", map[string]string{"warmup_hint": "true"}, false, true},
		{"false", map[string]string{"warmup_hint": "false"}, false, true},
		{"empty", map[string]string{"warmup_hint": ""}, false, false},
		{"not a bool", map[string]string{"warmup_hint": "async"}, false, false},
		{"refresh", map[string]string{"warmup_hint": "true"}, true, false},
		{"refresh without hint", map[string]string{"warmup_hint": "false"}, true, true},
		{"ordinary refresh", nil, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &LoadCollectionTask{LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
				CollectionName: "warmup_test", LoadParams: tc.params, Refresh: tc.refresh,
			}}
			err := task.PreExecute(context.Background())
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			}
		})
	}
}

func TestLoadCollectionWarmupPartialFieldIndexes(t *testing.T) {
	paramtable.Init()
	const db, coll = "default", "partial"
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: coll, Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "loaded", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "other", DataType: schemapb.DataType_FloatVector},
		},
	})
	for _, tc := range []struct {
		name    string
		fields  []string
		wantErr error
	}{
		{"ordinary load checks all fields", nil, merr.ErrParameterInvalid},
		{"explicitly load indexed fields", []string{"pk", "loaded"}, nil},
		{"explicitly load unindexed field", []string{"pk", "loaded", "other"}, merr.ErrParameterInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMockCache(t)
			cache.EXPECT().GetCollectionID(mock.Anything, db, coll).Return(int64(1), nil).Once()
			cache.EXPECT().GetCollectionSchema(mock.Anything, db, coll).Return(schema, nil).Once()
			mix := mocks.NewMockMixCoordClient(t)
			mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&indexpb.DescribeIndexResponse{
				Status: merr.Success(), IndexInfos: []*indexpb.IndexInfo{{FieldID: 101, IndexID: 201}},
			}, nil).Once()
			if tc.wantErr == nil {
				mix.EXPECT().LoadCollection(mock.Anything, mock.MatchedBy(func(req *querypb.LoadCollectionRequest) bool {
					return req.GetCollectionID() == 1 && len(req.GetLoadFields()) == 2 && req.GetFieldIndexID()[101] == 201
				})).Return(merr.Success(), nil).Once()
			}
			task := &LoadCollectionTask{LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
				Base: commonpbutil.NewMsgBase(), DbName: db, CollectionName: coll, LoadFields: tc.fields,
				LoadParams: map[string]string{common.WarmupHintKey: "true"},
			}, mixCoord: mix}
			task.MetaCache = cache
			require.NoError(t, task.PreExecute(t.Context()))
			require.ErrorIs(t, task.Execute(t.Context()), tc.wantErr)
		})
	}
}

func TestLoadCollectionWarmupAdmissionVersion(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name    string
		version string
		want    uint64
		failure bool
	}{
		{name: "accepted load", version: "42", want: 42},
		{name: "older coordinator"},
		{name: "malformed metadata", version: "invalid"},
		{name: "overflow", version: "18446744073709551616"},
		{name: "failed load", failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMockCache(t)
			cache.EXPECT().GetCollectionID(mock.Anything, "default", "warmup_test").Return(int64(100), nil).Once()
			cache.EXPECT().GetCollectionSchema(mock.Anything, "default", "warmup_test").Return(mustNewSchemaInfo(&schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
			}), nil).Once()
			mix := mocks.NewMockMixCoordClient(t)
			mix.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&indexpb.DescribeIndexResponse{Status: merr.Success()}, nil).Once()
			status := merr.Success()
			if tc.failure {
				status = merr.Status(merr.WrapErrServiceNotReadyMsg("injected admission failure"))
			}
			status.ExtraInfo = map[string]string{"existing": "preserved"}
			if tc.version != "" {
				status.ExtraInfo[common.LoadInfoVersionKey] = tc.version
			}
			mix.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(status, nil).Once()
			task := &LoadCollectionTask{LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
				Base: commonpbutil.NewMsgBase(), DbName: "default", CollectionName: "warmup_test",
				LoadParams: map[string]string{common.WarmupHintKey: "true"},
			}, mixCoord: mix}
			task.MetaCache = cache
			require.NoError(t, task.PreExecute(t.Context()))
			require.NoError(t, task.Execute(t.Context()))
			collectionID, version, requested := task.PrewarmTarget()
			require.EqualValues(t, 100, collectionID)
			require.Equal(t, tc.want, version)
			require.True(t, requested)
			require.Equal(t, map[string]string{"existing": "preserved"}, task.result.GetExtraInfo())
			require.Equal(t, !tc.failure, task.result.GetErrorCode() == commonpb.ErrorCode_Success)
		})
	}
}

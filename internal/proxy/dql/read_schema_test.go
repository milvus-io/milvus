package dql

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestInternalQueryKeepsParentReadSchema(t *testing.T) {
	parent := createTestSchema()
	parent.Version = 0 // zero remains an exact schema identity
	parent.Properties = []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: "30"}}
	newSchema := proto.Clone(parent.CollectionSchema).(*schemapb.CollectionSchema)
	newSchema.Version = 1
	newSchema.Properties = []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: "60"}}
	latest := mustNewSchemaInfo(newSchema)
	id := int64(1001)
	lookup := mockey.Mock((*metacache.MetaCache).GetCollectionID).To(func(*metacache.MetaCache, context.Context, string, string) (int64, error) { return id, nil }).Build()
	defer lookup.UnPatch()
	info := mockey.Mock((*metacache.MetaCache).GetCollectionInfo).Return(&collectionInfo{CollID: id, Schema: latest}, nil).Build()
	defer info.UnPatch()
	partition := mockey.Mock(IsPartitionKeyMode).Return(false, nil).Build()
	defer partition.UnPatch()
	partitions := mockey.Mock(GetPartitionIDs).Return([]int64{1}, nil).Build()
	defer partitions.UnPatch()
	for _, recreated := range []bool{false, true} {
		t.Run(map[bool]string{false: "published_schema_advanced", true: "name_recreated"}[recreated], func(t *testing.T) {
			id = 1001
			if recreated {
				id++
			}
			base := time.Unix(1_700_000_000, 0)
			task := &QueryTask{
				baseTask: baseTask{MetaCache: newTestCache()}, ctx: t.Context(),
				RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{Timestamp: tsoutil.ComposeTSByTime(base)}},
				request:         &milvuspb.QueryRequest{CollectionName: "test_collection", Expr: "id in [1]", OutputFields: []string{"id"}},
			}
			task.BindReadSchema(1001, parent)
			err := task.PreExecute(t.Context())
			if recreated {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				return
			}
			require.NoError(t, err)
			require.Same(t, parent, task.schema)
			require.Zero(t, task.GetSchemaVersion())
			require.Equal(t, tsoutil.ComposeTSByTime(base.Add(-30*time.Second)), task.GetCollectionTtlTimestamps())
		})
	}
}

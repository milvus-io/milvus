// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadTasksPreservePreloadIntent(t *testing.T) {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "collection", EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, Name: "value", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	})
	for _, tc := range []struct {
		name        string
		fields      []string
		skipDynamic bool
		noIndex     bool
		want        []int64
		wantErr     string
	}{
		{name: "default follows schema", want: []int64{100, 101, 102, 103}},
		{name: "skip dynamic", skipDynamic: true, want: []int64{100, 101, 102}},
		{name: "explicit full fields", fields: []string{"pk", "vector", "value"}, want: []int64{100, 101, 102, 103}},
		{name: "explicit subset", fields: []string{"pk", "vector"}, skipDynamic: true, want: []int64{100, 101}},
		{name: "default still validates indexes", noIndex: true, wantErr: "there is no vector index"},
		{name: "invalid field", fields: []string{"missing"}, wantErr: "missing"},
	} {
		for _, partitions := range []bool{false, true} {
			kind := "collection"
			if partitions {
				kind = "partitions"
			}
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				defer mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1), nil).Build().UnPatch()
				defer mockey.Mock((*MetaCache).GetCollectionSchema).Return(schema, nil).Build().UnPatch()
				defer mockey.Mock((*MetaCache).GetPartitionID).Return(int64(10), nil).Build().UnPatch()
				indexes := []*indexpb.IndexInfo{{FieldID: 101, IndexID: 201}}
				if tc.noIndex {
					indexes = nil
				}
				defer mockey.Mock((*MixCoordMock).DescribeIndex).Return(&indexpb.DescribeIndexResponse{
					Status: merr.Success(), IndexInfos: indexes,
				}, nil).Build().UnPatch()
				var got []int64
				calls := 0
				defer mockey.Mock((*MixCoordMock).LoadCollection).To(func(_ *MixCoordMock, _ context.Context, req *querypb.LoadCollectionRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
					got = append([]int64(nil), req.GetLoadFields()...)
					calls++
					return merr.Success(), nil
				}).Build().UnPatch()
				defer mockey.Mock((*MixCoordMock).LoadPartitions).To(func(_ *MixCoordMock, _ context.Context, req *querypb.LoadPartitionsRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
					got = append([]int64(nil), req.GetLoadFields()...)
					calls++
					return merr.Success(), nil
				}).Build().UnPatch()
				var err error
				if partitions {
					task := &LoadPartitionsTask{LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
						Base: commonpbutil.NewMsgBase(), CollectionName: "collection", PartitionNames: []string{"p"},
						LoadFields: tc.fields, SkipLoadDynamicField: tc.skipDynamic,
					}, mixCoord: &MixCoordMock{}}
					task.MetaCache = &MetaCache{}
					err = task.Execute(t.Context())
				} else {
					task := &LoadCollectionTask{LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
						Base: commonpbutil.NewMsgBase(), CollectionName: "collection",
						LoadFields: tc.fields, SkipLoadDynamicField: tc.skipDynamic,
					}, mixCoord: &MixCoordMock{}}
					task.MetaCache = &MetaCache{}
					err = task.Execute(t.Context())
				}
				if tc.wantErr != "" {
					require.ErrorContains(t, err, tc.wantErr)
					require.Zero(t, calls)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				require.ElementsMatch(t, tc.want, got)
			})
		}
	}
}

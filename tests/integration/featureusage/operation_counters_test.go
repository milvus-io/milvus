// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package featureusage

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestOperationCounters walks the counters that count a client call rather
// than a request option: operations that leave nothing in the metadata to
// walk. Each counts once per call at the Proxy, whether or not the call then
// succeeds, so several cases only need the server to receive the request.
func (s *Suite) TestOperationCounters() {
	ctx := context.Background()
	s.ensureCollection(ctx)
	client := s.Cluster.MilvusClient

	name := "ops_" + funcutil.GenRandomStr()
	s.createSimpleCollection(ctx, name, 8)
	defer client.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})
	snapshot := "snap_" + funcutil.GenRandomStr()
	defer client.DropSnapshot(ctx, &milvuspb.DropSnapshotRequest{Name: snapshot, CollectionName: name})

	added, err := proto.Marshal(&schemapb.FieldSchema{
		Name: "added", DataType: schemapb.DataType_VarChar, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
	})
	s.Require().NoError(err)

	cases := []struct {
		name string
		run  func() error
		want map[string]int64
	}{
		{
			name: "list running requests",
			run: func() error {
				_, err := client.ListRunningRequests(ctx, &milvuspb.ListRunningRequestsRequest{})
				return err
			},
			want: map[string]int64{"running_requests=list": 1},
		},
		{
			name: "cancel an id nobody runs",
			run: func() error {
				_, err := client.CancelRequests(ctx, &milvuspb.CancelRequestsRequest{RequestIds: []string{"no-such-request"}})
				return err
			},
			want: map[string]int64{"running_requests=cancel": 1},
		},
		{
			name: "add a field after creation",
			run: func() error {
				_, err := client.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{CollectionName: name, Schema: added})
				return err
			},
			want: map[string]int64{"schema_change=add_field": 1},
		},
		{
			name: "alter schema",
			run: func() error {
				_, err := client.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{CollectionName: name})
				return err
			},
			want: map[string]int64{"schema_change=alter_schema": 1},
		},
		{
			name: "alter a function",
			run: func() error {
				_, err := client.AlterCollectionFunction(ctx, &milvuspb.AlterCollectionFunctionRequest{
					CollectionName: name, FunctionName: "no_such_function",
				})
				return err
			},
			want: map[string]int64{"schema_change=alter_function": 1},
		},
		{
			name: "add a file resource",
			run: func() error {
				_, err := client.AddFileResource(ctx, &milvuspb.AddFileResourceRequest{Name: "res_" + funcutil.GenRandomStr(), Path: "no/such/file"})
				return err
			},
			want: map[string]int64{"file_resource=add": 1},
		},
		{
			name: "create a snapshot",
			run: func() error {
				_, err := client.CreateSnapshot(ctx, &milvuspb.CreateSnapshotRequest{Name: snapshot, CollectionName: name})
				return err
			},
			want: map[string]int64{"snapshot=create": 1},
		},
		{
			name: "export a snapshot",
			run: func() error {
				_, err := client.ExportSnapshot(ctx, &milvuspb.ExportSnapshotRequest{Name: "no_such_snapshot", CollectionName: name})
				return err
			},
			want: map[string]int64{"snapshot=export": 1},
		},
		{
			name: "restore a snapshot",
			run: func() error {
				_, err := client.RestoreSnapshot(ctx, &milvuspb.RestoreSnapshotRequest{
					Name: "no_such_snapshot", CollectionName: name, TargetCollectionName: name + "_restored",
				})
				return err
			},
			want: map[string]int64{"snapshot=restore": 1},
		},
		{
			name: "restore an external snapshot counts as a restore",
			run: func() error {
				_, err := client.RestoreExternalSnapshot(ctx, &milvuspb.RestoreExternalSnapshotRequest{TargetCollectionName: name + "_external"})
				return err
			},
			want: map[string]int64{"snapshot=restore": 1},
		},
		{
			name: "truncate",
			run: func() error {
				_, err := client.TruncateCollection(ctx, &milvuspb.TruncateCollectionRequest{CollectionName: name})
				return err
			},
			want: map[string]int64{"truncate_collection": 1},
		},
		{
			name: "query with a function chain",
			run: func() error {
				s.queryWith(ctx, func(r *milvuspb.QueryRequest) {
					r.FunctionChains = []*schemapb.FunctionChain{{
						Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan,
						Ops:   []*schemapb.FunctionChainOp{{Op: "map", Expr: &schemapb.FunctionChainExpr{Name: "round_decimal"}}},
					}}
				})
				return nil
			},
			want: map[string]int64{"query_function_chains": 1},
		},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			before := counters(s.report(ctx), typeutil.ProxyRole)
			s.Require().NoError(tc.run(), "transport error")
			requireOnlyDelta(s.T(), before, counters(s.report(ctx), typeutil.ProxyRole), tc.want)
		})
	}
}

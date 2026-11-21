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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func run() error {
	endpoint := flag.String("endpoint", "", "worker RPC endpoint")
	term := flag.Int64("term", 0, "PChannel ownership term")
	tick := flag.Uint64("tick", 0, "admitted read snapshot")
	flag.Parse()
	payload, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	var meta viewpb.QueryViewMeta
	if err = protojson.Unmarshal(payload, &meta); err != nil {
		return err
	}
	plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 10}}, OutputFieldIds: []int64{100, 101}})
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = handler.EncodeQueryViewPChannelToOutgoingContext(ctx, types.PChannelInfo{Name: funcutil.ToPhysicalChannel(meta.Vchannel), Term: *term, AccessMode: types.AccessModeRW})
	req := &viewpb.QueryOnViewRequest{
		ShardId:   &viewpb.ShardID{ReplicaId: meta.ReplicaId, Vchannel: meta.Vchannel},
		Version:   meta.Version,
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: *tick, TransformingTimetick: *tick},
		LegacyReq: &internalpb.RetrieveRequest{CollectionID: meta.CollectionId, SchemaVersion: meta.SchemaVersion, MvccTimestamp: *tick, SerializedExprPlan: plan, OutputFieldsId: []int64{100, 101}, Limit: 10},
	}
	resp, err := viewpb.NewViewQueryServiceClient(conn).QueryOnView(ctx, req)
	if err != nil {
		return viewerror.ConvertViewError("QueryOnView", err)
	}
	data, err := protojson.Marshal(resp)
	if err == nil {
		fmt.Println(string(data))
	}
	return err
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

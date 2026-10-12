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

package taskmodel

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
)

// TaskNode is the contract a concrete task needs from its host proxy node.
// It is implemented by the proxy composition root and consumed by the concrete
// task structs (dql/dml/ddl packages). It deliberately has no Query or Sched
// methods — those would reference DQL types or the scheduler package and create
// an import cycle with the model layer.
type TaskNode interface {
	GetMetaCache() metacache.Cache
	MixCoord() types.MixCoordClient
	LBPolicy() shardclient.LBPolicy
	ShardMgr() shardclient.ShardClientMgr
	ChMgr() channelmgr.ChannelsMgr
	TsoAllocator() TsoAllocator
	ResolveRLSEnforcement(ctx context.Context, cache metacache.Cache, rlsEnabled, rlsForce, skipRLS bool, dbName, collectionName, operation string) (bool, error)
}

// QueryRunner executes a query task. Implemented by the proxy composition root;
// the concrete task type is asserted inside the implementation. The method is
// named ExecuteQuery (not Query) because *Proxy already owns a Query gRPC
// handler with a different signature and Go forbids two same-named methods.
type QueryRunner interface {
	ExecuteQuery(ctx context.Context, qt Task, sp trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error)
}

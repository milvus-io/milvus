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

package extension

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

// Coordinator is the whole coordinator as its own clients see it: every
// root, query and data coordinator RPC. An engine reaches the coordinator
// through nothing else, so what it can do is exactly what a proxy can do.
type Coordinator interface {
	rootcoordpb.RootCoordClient
	querypb.QueryCoordClient
	datapb.DataCoordClient
}

// CoordinatorEngine is control-plane machinery a deployment hosts inside the
// coordinator process. The coordinator starts it once this replica becomes
// ACTIVE (a standby never starts it) and stops it on shutdown; between the two
// the engine drives the coordinator through coord.
type CoordinatorEngine interface {
	Start(ctx context.Context, coord Coordinator) error
	Stop() error
}

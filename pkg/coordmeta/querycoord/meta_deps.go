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

package querycoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// Broker abstracts the single coordinator call CollectionManager needs during
// recovery (filling load fields from the collection schema). The full broker
// implementation lives in internal/querycoordv2/meta (CoordinatorBroker, which
// depends on internal-only RPC plumbing); declaring this minimal interface here
// keeps the moved CollectionManager free of internal imports so it can live in
// the shared pkg/v3 module. The production *CoordinatorBroker satisfies it
// structurally.
type Broker interface {
	DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
}

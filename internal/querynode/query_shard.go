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

package querynode

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type queryShard struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID
	channel      Channel
}

func newQueryShard(
	ctx context.Context,
	collectionID UniqueID,
	channel Channel,
) *queryShard {
	ctx, cancel := context.WithCancel(ctx)
	qs := &queryShard{
		ctx:          ctx,
		cancel:       cancel,
		collectionID: collectionID,
		channel:      channel,
	}
	return qs
}

func (q *queryShard) search(ctx context.Context, req *querypb.SearchRequest) (*milvuspb.SearchResults, error) {
	return nil, errors.New("not implemented")
}

func (q *queryShard) query(ctx context.Context, req *querypb.QueryRequest) (*milvuspb.QueryResults, error) {
	return nil, errors.New("not implemented")
}

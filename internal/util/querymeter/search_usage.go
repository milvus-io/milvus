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

package querymeter

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

type searchUsageKey struct{}

// SearchUsage accumulates successful read stages for one outer Search RPC.
type SearchUsage struct {
	mu     sync.Mutex
	read   internalpb.QueryReadUsage
	schema *schemapb.CollectionSchema
}

func WithSearchUsage(ctx context.Context) (context.Context, *SearchUsage) {
	usage := &SearchUsage{}
	return context.WithValue(ctx, searchUsageKey{}, usage), usage
}

func SearchUsageFromContext(ctx context.Context) *SearchUsage {
	usage, _ := ctx.Value(searchUsageKey{}).(*SearchUsage)
	return usage
}

func (u *SearchUsage) AddRead(part *internalpb.QueryReadUsage) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return AddReadUsage(&u.read, part)
}

func (u *SearchUsage) SetResultSchema(schema *schemapb.CollectionSchema) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.schema = schema
}

func (u *SearchUsage) ResultSchema() *schemapb.CollectionSchema {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.schema
}

func (u *SearchUsage) Bytes(returned uint64) (int64, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return BillableBytes(&u.read, returned)
}

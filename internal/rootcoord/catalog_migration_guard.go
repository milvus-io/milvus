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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func (c *Core) beginCatalogMigrationProtectedMetadataWrite(ctx context.Context) (func(), error) {
	if c.migrationGate == nil {
		c.migrationGate = newCatalogMigrationGate()
	}
	return c.migrationGate.BeginMetadataWrite(ctx)
}

func (c *Core) startCatalogMigrationDraining() {
	if c.migrationGate == nil {
		c.migrationGate = newCatalogMigrationGate()
	}
	c.migrationGate.StartDraining()
}

func (c *Core) waitCatalogMigrationDrained(ctx context.Context) error {
	if c.migrationGate == nil {
		return nil
	}
	return c.migrationGate.WaitDrained(ctx)
}

func (c *Core) resumeCatalogMigrationWrites() {
	if c.migrationGate == nil {
		return
	}
	c.migrationGate.Resume()
}

func (c *Core) protectCatalogMigrationBroadcast(ctx context.Context, api broadcaster.BroadcastAPI) (broadcaster.BroadcastAPI, error) {
	done, err := c.beginCatalogMigrationProtectedMetadataWrite(ctx)
	if err != nil {
		if api != nil {
			api.Close()
		}
		return nil, err
	}
	return &migrationProtectedBroadcastAPI{BroadcastAPI: api, done: done}, nil
}

type migrationProtectedBroadcastAPI struct {
	broadcaster.BroadcastAPI
	done func()
}

func (b *migrationProtectedBroadcastAPI) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	return b.BroadcastAPI.Broadcast(ctx, msg)
}

func (b *migrationProtectedBroadcastAPI) Close() {
	b.BroadcastAPI.Close()
	if b.done != nil {
		b.done()
		b.done = nil
	}
}

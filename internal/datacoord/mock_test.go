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

package datacoord

import (
	"testing"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
)

// newMemoryMetaWithDomainMetas builds a meta over an in-memory catalog and returns the domain metas built with it.
func newMemoryMetaWithDomainMetas(t *testing.T) (*meta.Meta, *domainMetas, error) {
	dm := &domainMetas{}
	m, err := meta.NewMemoryMetaWithLoader(t, func(catalog metastore.DataCoordCatalog, cm storage.ChunkManager) meta.DomainMetaLoader {
		return loadDomainMetas(catalog, cm, dm)
	})
	return m, dm, err
}

type mockCompactionTrigger struct {
	methods map[string]interface{}
}

// triggerSingleCompaction trigerr a compaction bundled with collection-partiiton-channel-segment
func (t *mockCompactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, blockToSendSignal bool) error {
	if f, ok := t.methods["triggerSingleCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64, partitionID int64, segmentID int64, channel string) error); ok {
			return ff(collectionID, partitionID, segmentID, channel)
		}
	}
	panic("not implemented")
}

// triggerManualCompaction force to start a compaction
func (t *mockCompactionTrigger) triggerManualCompaction(collectionID int64) (UniqueID, error) {
	if f, ok := t.methods["triggerManualCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64) (UniqueID, error)); ok {
			return ff(collectionID)
		}
	}
	panic("not implemented")
}

func (t *mockCompactionTrigger) Start() {
	if f, ok := t.methods["start"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

func (t *mockCompactionTrigger) Stop() {
	if f, ok := t.methods["stop"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

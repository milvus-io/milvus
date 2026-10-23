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

package task

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/datacoord/analyze"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/partitionstats"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
)

// compactionTaskTestMetas holds the domain metas that compaction task tests build together with a meta.
type compactionTaskTestMetas struct {
	indexMeta          *index.IndexMeta
	analyzeMeta        *analyze.AnalyzeMeta
	partitionStatsMeta *partitionstats.PartitionStatsMeta
	compactionTaskMeta *CompactionTaskMeta
	snapshotMeta       *snapshot.SnapshotMeta
}

// loadCompactionTaskTestMetas returns a DomainMetaLoader that builds the index, analyze, partition stats,
// compaction task and snapshot metas into out. It returns the index meta as the index catalog and the
// snapshot meta as the compaction protection.
func loadCompactionTaskTestMetas(catalog metastore.DataCoordCatalog, cm storage.ChunkManager, out *compactionTaskTestMetas) meta.DomainMetaLoader {
	return func(ctx context.Context, collectionIDs []int64) (meta.SegmentIndexCatalog, meta.CompactionProtection, error) {
		var err error
		if out.indexMeta, err = index.NewIndexMeta(ctx, catalog, collectionIDs); err != nil {
			return nil, nil, err
		}
		if out.analyzeMeta, err = analyze.NewAnalyzeMeta(ctx, catalog); err != nil {
			return nil, nil, err
		}
		if out.partitionStatsMeta, err = partitionstats.NewPartitionStatsMeta(ctx, catalog); err != nil {
			return nil, nil, err
		}
		if out.compactionTaskMeta, err = NewCompactionTaskMeta(ctx, catalog); err != nil {
			return nil, nil, err
		}
		if out.snapshotMeta, err = snapshot.NewSnapshotMeta(ctx, catalog, cm); err != nil {
			return nil, nil, err
		}
		return out.indexMeta, out.snapshotMeta, nil
	}
}

// withCompactionTaskTestMetas returns a factory of loaders that build the compaction-task-layer domain metas into out.
func withCompactionTaskTestMetas(out *compactionTaskTestMetas) func(metastore.DataCoordCatalog, storage.ChunkManager) meta.DomainMetaLoader {
	return func(catalog metastore.DataCoordCatalog, cm storage.ChunkManager) meta.DomainMetaLoader {
		return loadCompactionTaskTestMetas(catalog, cm, out)
	}
}

// newMemoryMetaWithCompactionTask builds a meta over an in-memory catalog together with the compaction-task-layer domain metas.
func newMemoryMetaWithCompactionTask(t *testing.T) (*meta.Meta, *compactionTaskTestMetas, error) {
	dm := &compactionTaskTestMetas{}
	m, err := meta.NewMemoryMetaWithLoader(t, withCompactionTaskTestMetas(dm))
	return m, dm, err
}

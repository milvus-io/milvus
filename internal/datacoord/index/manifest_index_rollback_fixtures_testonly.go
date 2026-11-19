// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package index

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func SetRollbackTestParam(t *testing.T, item *paramtable.ParamItem, value string) {
	t.Helper()
	previous := item.GetValue()
	Params.Save(item.Key, value)
	t.Cleanup(func() { Params.Save(item.Key, previous) })
}

func WithManifestIndexRollback(t *testing.T, enabled bool) {
	t.Helper()
	SetRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackEnabled, strconv.FormatBool(enabled))
}

func RollbackFixture(t *testing.T) (*meta.Meta, *metastorekv.Catalog, *meta.FakeManifestStore, *FailBackfillCatalogKV) {
	t.Helper()
	meta.WithSegmentIndexManifestWrites(t, false)
	WithManifestIndexRollback(t, false)
	store := meta.NewFakeManifestStore(t)
	kv := &FailBackfillCatalogKV{MetaMemoryKV: meta.NewMetaMemoryKV()}
	catalog := metastorekv.NewCatalog(kv, "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	require.NoError(t, NewMigrationBackfillInspector(context.TODO(), m).backfillIndexes(context.TODO(), meta.RestartSegID, meta.RestartBuildID))
	WithManifestIndexRollback(t, true)
	return m, catalog, store, kv
}

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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type indexModeStartupCatalog struct {
	metastore.DataCoordCatalog
	modeChecked bool
}

func (c *indexModeStartupCatalog) EnsureIndexMetadataMode(context.Context, bool, bool) error {
	c.modeChecked = true
	return nil
}

func TestSummaryModeRejectsV2BeforePersistingMode(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentIndexSummariesEnabled, "true")
	setSegmentChangeTestParam(t, &Params.CommonCfg.UseLoonFFI, "false")
	catalog := &indexModeStartupCatalog{}
	server := &Server{ctx: t.Context(), catalog: catalog}
	err := server.initMeta(nil)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	require.ErrorContains(t, err, "common.storage.useLoonFFI=true")
	require.False(t, catalog.modeChecked)
}

func TestIndexMetadataModeStartupValidation(t *testing.T) {
	mode := &Params.DataCoordCfg.SegmentIndexSummariesEnabled
	setSegmentChangeTestParam(t, mode, "ture")
	setSegmentChangeTestParam(t, &Params.CommonCfg.UseLoonFFI, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.WriteSegmentIndexToManifest, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.ManifestIndexBackfillEnabled, "false")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackEnabled, "false")
	ctx := t.Context()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: Params.EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()
	kv := etcdkv.NewEtcdKV(client, "index-mode-startup-"+uuid.NewString()+"/meta")
	defer func() { _ = kv.RemoveWithPrefix(ctx, "") }()
	catalog := kvdatacoord.NewCatalog(kv, "", "")
	modeKey := kvdatacoord.MetaPrefix + "/index-metadata-mode"
	startup := func() error {
		s := &Server{ctx: ctx, catalog: catalog, metaStore: metacache.NewMetaStore(catalog)}
		return s.initIndexMetadataMode()
	}
	err = startup()
	require.ErrorContains(t, err, "requires a boolean value")
	_, err = kv.Load(ctx, modeKey)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)

	Params.Save(mode.Key, "true")
	err = startup()
	expected := "segment-summary-v1"
	if !segmentIndexSummaryReadersReady {
		require.ErrorContains(t, err, "require summary-aware readers and garbage collection")
		_, err = kv.Load(ctx, modeKey)
		require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
		Params.Save(mode.Key, "false")
		err = startup()
		expected = "legacy"
	}
	require.NoError(t, err)
	require.NoError(t, startup())
	persisted, err := kv.Load(ctx, modeKey)
	require.NoError(t, err)
	require.Equal(t, expected, persisted)
}

func TestSummaryModeRejectsDirectManifestAdoption(t *testing.T) {
	key := Params.DataCoordCfg.SegmentIndexSummariesEnabled.Key
	previous := Params.DataCoordCfg.SegmentIndexSummariesEnabled.GetValue()
	Params.Save(key, "true")
	t.Cleanup(func() { Params.Save(key, previous) })
	status, err := (&Server{}).BatchUpdateManifest(t.Context(), &datapb.BatchUpdateManifestRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceUnimplemented)
}

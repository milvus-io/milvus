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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
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

func TestSummaryModeRejectsDirectManifestAdoption(t *testing.T) {
	key := Params.DataCoordCfg.SegmentIndexSummariesEnabled.Key
	previous := Params.DataCoordCfg.SegmentIndexSummariesEnabled.GetValue()
	Params.Save(key, "true")
	t.Cleanup(func() { Params.Save(key, previous) })
	status, err := (&Server{}).BatchUpdateManifest(t.Context(), &datapb.BatchUpdateManifestRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceUnimplemented)
}

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

package dataview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRecoveryStartsWorkerOnlyAfterSuccess(t *testing.T) {
	for _, mode := range []string{"cleanup-failure", "live-validator-failure", "success"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			catalog := datacoord.NewCatalog(nil, "", "")
			list := mockey.Mock((*datacoord.Catalog).ListAllDataViews).Return([]*viewpb.DataViewOfCollection{
				{CollectionId: 1, DataVersion: version(1, 0)},
			}, nil).Build()
			defer list.UnPatch()
			recoveryErr := merr.WrapErrServiceUnavailable("recovery failed")
			var cleanupErr error
			if mode == "cleanup-failure" {
				cleanupErr = recoveryErr
			}
			drop := mockey.Mock((*datacoord.Catalog).DropDataViews).Return(cleanupErr).Build()
			defer drop.UnPatch()
			validatedLiveCollection := false
			start := mockey.Mock((*dataViewManager).startWorker).To(func(*dataViewManager) {
				require.True(t, validatedLiveCollection, "worker started before recovery completed")
			}).Build()
			defer start.UnPatch()
			manager, err := RecoverManager(ctx, catalog, func(_ context.Context, collectionID int64) (bool, error) {
				if collectionID == 2 {
					if mode == "live-validator-failure" {
						return false, recoveryErr
					}
					validatedLiveCollection = true
				}
				return false, nil
			}, projectSegments(), []int64{2}, nil)
			require.Equal(t, 1, drop.Times())
			if mode == "success" {
				require.NoError(t, err)
				require.NotNil(t, manager)
				require.Equal(t, 1, start.Times())
			} else {
				require.ErrorIs(t, err, recoveryErr)
				require.Nil(t, manager)
				require.Zero(t, start.Times(), "failed recovery must not retain a worker")
			}
		})
	}
}

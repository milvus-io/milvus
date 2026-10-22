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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestCheckCheckPointsHealth(t *testing.T) {
	paramtable.Init()
	maxLag := paramtable.Get().DataCoordCfg.ChannelCheckpointMaxLag.GetAsDuration(time.Second)
	const channel = "by-dev-rootcoord-dml_0_100v0"
	tests := []struct {
		name    string
		coll    *collectionInfo
		wantErr error
	}{
		{"created collection lags", &collectionInfo{ID: 100}, merr.ErrChannelCPExceededMaxLag},
		// A dropped collection's checkpoint stops advancing until GC.
		{"dropping collection", &collectionInfo{ID: 100, State: etcdpb.CollectionState_CollectionDropping}, nil},
		{"missing collection", nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			store := metacache.NewMetaStore(catalog)
			if tc.coll != nil {
				store.PutCollection(tc.coll)
			}
			m := &meta{metaStore: store}
			require.NoError(t, store.UpdateChannelCheckpoint(context.Background(), channel, &msgpb.MsgPosition{
				ChannelName: channel,
				MsgID:       []byte{1},
				Timestamp:   tsoutil.ComposeTSByTime(time.Now().Add(-2 * maxLag)),
			}))
			err := CheckCheckPointsHealth(m)
			if tc.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.wantErr)
			}
		})
	}
}

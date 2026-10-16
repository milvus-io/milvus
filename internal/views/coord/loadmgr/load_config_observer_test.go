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

package loadmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadConfigObserverOnlyReportsCommittedChanges(t *testing.T) {
	store, catalog := newTestStore(t)
	var events []bool
	store.RegisterObserver(func(id int64, released bool) {
		require.EqualValues(t, 100, id)
		events = append(events, released)
	})
	cfg := sampleConfig()
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(merr.WrapErrServiceUnavailableMsg("catalog unavailable")).Once()
	require.Error(t, store.Put(context.Background(), cfg))
	require.Empty(t, events)
	expectFullSave(catalog, 2)
	require.NoError(t, store.Put(context.Background(), cfg))
	require.NoError(t, store.Put(context.Background(), cfg))
	catalog.EXPECT().ReleaseReplicas(mock.Anything, int64(100)).Return(nil).Once()
	catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil).Once()
	require.NoError(t, store.Remove(context.Background(), 100))
	require.NoError(t, store.Remove(context.Background(), 100))
	require.Equal(t, []bool{false, false, true}, events)
}

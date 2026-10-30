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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv/mocks"
)

func TestRemoveDroppedChannelFlags(t *testing.T) {
	t.Run("removes only removed flags", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		flags := map[string]string{
			"live_channel":     NonRemoveFlagTomestone,
			"legacy_channel_1": RemoveFlagTomestone,
			"legacy_channel_2": RemoveFlagTomestone,
		}
		txn.EXPECT().WalkWithPrefix(mock.Anything, ChannelRemovePrefix+"/", mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, prefix string, _ int, fn func([]byte, []byte) error) error {
				for channel, value := range flags {
					// The walk reports keys under the meta root path.
					if err := fn([]byte("root/"+prefix+channel), []byte(value)); err != nil {
						return err
					}
				}
				return nil
			})
		var removed []string
		txn.EXPECT().MultiRemove(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, keys []string) error {
				removed = append(removed, keys...)
				return nil
			})
		catalog := NewCatalog(txn, "root", "")

		count, err := catalog.RemoveDroppedChannelFlags(context.TODO())
		require.NoError(t, err)
		assert.Equal(t, 2, count)
		// Only the "removed" flags go; the live channel keeps its flag.
		assert.ElementsMatch(t, []string{
			buildChannelRemovePath("legacy_channel_1"),
			buildChannelRemovePath("legacy_channel_2"),
		}, removed)
	})

	t.Run("walk fails", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error"))
		catalog := NewCatalog(txn, "root", "")
		_, err := catalog.RemoveDroppedChannelFlags(context.TODO())
		assert.Error(t, err)
	})

	t.Run("remove fails", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, prefix string, _ int, fn func([]byte, []byte) error) error {
				return fn([]byte(prefix+"ch1"), []byte(RemoveFlagTomestone))
			})
		txn.EXPECT().MultiRemove(mock.Anything, []string{buildChannelRemovePath("ch1")}).Return(errors.New("mock error"))
		catalog := NewCatalog(txn, "root", "")
		_, err := catalog.RemoveDroppedChannelFlags(context.TODO())
		assert.Error(t, err)
	})
}

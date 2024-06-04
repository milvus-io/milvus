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

package binlog

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestL0Reader_NewL0Reader(t *testing.T) {
	ctx := context.Background()

	t.Run("normal", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		r, err := NewL0Reader(ctx, cm, nil, &internalpb.ImportFile{Paths: []string{"mock-prefix"}}, 100)
		assert.NoError(t, err)
		assert.NotNil(t, r)
	})

	t.Run("invalid path", func(t *testing.T) {
		r, err := NewL0Reader(ctx, nil, nil, &internalpb.ImportFile{Paths: []string{"mock-prefix", "mock-prefix2"}}, 100)
		assert.Error(t, err)
		assert.Nil(t, r)
	})

	t.Run("list failed", func(t *testing.T) {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error"))
		r, err := NewL0Reader(ctx, cm, nil, &internalpb.ImportFile{Paths: []string{"mock-prefix"}}, 100)
		assert.Error(t, err)
		assert.Nil(t, r)
	})
}

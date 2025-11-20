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

package rootcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

func TestNewKeyManager(t *testing.T) {
	ctx := context.Background()
	meta := mockrootcoord.NewIMetaTable(t)
	hookutil.InitTestCipher()

	km := NewKeyManager(ctx, meta)

	assert.NotNil(t, km)
	assert.Equal(t, ctx, km.ctx)
	assert.Equal(t, meta, km.meta)
}

func TestKeyManager_GetDatabaseByEzID(t *testing.T) {
	ctx := context.Background()
	hookutil.InitTestCipher()

	t.Run("success get database", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)

		expectedDB := &model.Database{
			ID:   123,
			Name: "test_db",
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(expectedDB, nil).Once()

		km := &KeyManager{
			ctx:  ctx,
			meta: meta,
		}

		db, err := km.getDatabaseByEzID(123)
		assert.NoError(t, err)
		assert.Equal(t, expectedDB, db)
	})

	t.Run("fallback to default database", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)

		defaultDB := &model.Database{
			ID:   util.DefaultDBID,
			Name: util.DefaultDBName,
		}

		meta.EXPECT().GetDatabaseByID(ctx, int64(123), uint64(0)).Return(nil, errors.New("db not found")).Once()
		meta.EXPECT().GetDatabaseByID(ctx, util.DefaultDBID, uint64(0)).Return(defaultDB, nil).Once()

		km := &KeyManager{
			ctx:  ctx,
			meta: meta,
		}

		db, err := km.getDatabaseByEzID(123)
		assert.NoError(t, err)
		assert.Equal(t, defaultDB, db)
	})
}

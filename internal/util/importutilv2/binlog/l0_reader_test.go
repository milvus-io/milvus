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
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
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

func TestL0Reader_Read(t *testing.T) {
	ctx := context.Background()
	const (
		delCnt = 100
	)

	deleteData := storage.NewDeleteData(nil, nil)
	for i := 0; i < delCnt; i++ {
		deleteData.Append(storage.NewVarCharPrimaryKey(fmt.Sprintf("No.%d", i)), uint64(i+1))
	}
	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(1, 2, 3, deleteData)
	assert.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, walkFunc storage.ChunkObjectWalkFunc) error {
			for _, file := range []string{"a/b/c/"} {
				walkFunc(&storage.ChunkObjectInfo{FilePath: file})
			}
			return nil
		})
	cm.EXPECT().Read(mock.Anything, mock.Anything).Return(blob.Value, nil)

	r, err := NewL0Reader(ctx, cm, nil, &internalpb.ImportFile{Paths: []string{"mock-prefix"}}, 100)
	assert.NoError(t, err)

	res, err := r.Read()
	assert.NoError(t, err)
	assert.Equal(t, int64(delCnt), res.RowCount)
	assert.Equal(t, deleteData.Size(), res.Size())

	_, err = r.Read()
	assert.Error(t, err)
	assert.ErrorIs(t, err, io.EOF)
}

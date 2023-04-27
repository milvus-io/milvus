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
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
)

const (
	TempFilesPath = "/tmp/milvus_test/rootcoord/"
)

type mockMetaTableSimple struct {
	IMetaTable
}

func (m mockMetaTableSimple) GetCollectionByID(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	if collectionID == 1 {
		return &model.Collection{
			CollectionID: 1,
			Name:         "dummy",
		}, nil
	}

	return nil, errors.New("collection not found")
}

func (m mockMetaTableSimple) GetPartitionNameByID(collID UniqueID, partitionID UniqueID, ts Timestamp) (string, error) {
	if collID == 1 && partitionID == 2 {
		return "noname", nil
	}

	return "", errors.New("partition not found")
}

func TestImportHelper_GetCollectionName(t *testing.T) {
	c := &Core{
		meta: &mockMetaTableSimple{},
	}

	getNameFunc := GetCollectionNameWithCore(c)
	colName, partName, err := getNameFunc(0, 0)
	assert.Empty(t, colName)
	assert.Empty(t, partName)
	assert.Error(t, err)

	colName, partName, err = getNameFunc(1, 0)
	assert.NotEmpty(t, colName)
	assert.Empty(t, partName)
	assert.Error(t, err)

	colName, partName, err = getNameFunc(1, 2)
	assert.NotEmpty(t, colName)
	assert.NotEmpty(t, partName)
	assert.NoError(t, err)
}

func TestImportHelper_CheckDiskQuota(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(TempFilesPath)

	ctx := context.Background()
	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	c := &Core{
		chunkManager: cm,
		quotaCenter:  &QuotaCenter{},
	}

	checkFunc := CheckDiskQuotaWithCore(c)
	filePath1 := TempFilesPath + "a.json"
	filePath2 := TempFilesPath + "b.json"
	files := []string{filePath1, filePath2}

	// file not found
	err = checkFunc(ctx, 1, files)
	assert.Error(t, err)

	content := []byte(`{
		"rows":[
			{"FieldBool": true, "FieldInt8": false, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4]},
		]
	}`)

	err = cm.Write(ctx, filePath1, content)
	assert.NoError(t, err)

	err = cm.Write(ctx, filePath2, content)
	assert.NoError(t, err)

	// file size less than max size
	err = checkFunc(ctx, 1, files)
	assert.NoError(t, err)
}

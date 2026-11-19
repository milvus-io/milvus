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

package dml

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestInsertTaskPreExecuteRejectsBlob(t *testing.T) {
	metacache.MockWriteSchemaForTest(t)
	const (
		dbName         = "db"
		collectionName = "blob_collection"
	)
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: testInt64Field, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "blob", DataType: schemapb.DataType_Blob},
		},
	})
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, dbName, collectionName).Return(int64(100), nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, dbName, collectionName, int64(100)).Return(&collectionInfo{Schema: schema}, nil)
	cache.EXPECT().GetCollectionSchema(mock.Anything, dbName, collectionName).Return(schema, nil).Maybe()
	task := &InsertTask{
		ctx: context.Background(),
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
				DbName:         dbName,
				CollectionName: collectionName,
				NumRows:        1,
			},
		},
	}
	task.MetaCache = cache

	err := task.PreExecute(context.Background())
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.ErrorContains(t, err, "writing BLOB data is not supported yet")
}

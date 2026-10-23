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

package numpy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
)

// Numpy files that name no field of the collection cannot produce a row; with
// no field reader, Read returned empty batches without EOF and the import task
// looped forever. The check runs before any file is opened.
func TestCreateReadersRejectsFilesWithNoCollectionField(t *testing.T) {
	collection := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", IsPrimaryKey: true, AutoID: true, DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int32, Nullable: true},
	}}
	cm := mocks.NewChunkManager(t)
	_, err := CreateReaders(context.Background(), cm, collection, []string{"/tmp/other.npy", "/tmp/unknown.npy"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "provide no field of the collection")
	_, err = CreateReaders(context.Background(), cm, collection, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "provide no field of the collection")
}

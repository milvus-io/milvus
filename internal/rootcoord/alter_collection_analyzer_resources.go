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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	streamingbroadcaster "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
)

func (c *Core) prepareAlterCollectionAnalyzerFileResources(ctx context.Context, coll *model.Collection, schema *schemapb.CollectionSchema) ([]int64, error) {
	fileResourceIds, err := c.validateSchemaAnalyzerFileResources(ctx, schema)
	if err != nil {
		return nil, err
	}
	schema.FileResourceIds = fileResourceIds

	addedFileResourceIds, _ := diffFileResourceIDs(coll.FileResourceIds, schema.FileResourceIds)
	if err := reserveAlterCollectionFileResourceRefs(c.meta, coll.CollectionID, addedFileResourceIds); err != nil {
		return nil, err
	}
	return addedFileResourceIds, nil
}

func rollbackAlterCollectionAnalyzerFileResourceReservation(ctx context.Context, meta IMetaTable, collectionID int64, ids []int64, broadcastErr error) {
	if streamingbroadcaster.IsBroadcastTaskNotCreated(broadcastErr) {
		rollbackAlterCollectionFileResourceRefs(ctx, meta, collectionID, ids)
	}
}

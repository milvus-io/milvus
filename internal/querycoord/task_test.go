// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package querycoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func TestTriggerTask(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	t.Run("Test LoadCollection", func(t *testing.T) {
		req := &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadCollection,
			},
			CollectionID: defaultCollectionID,
			Schema:       genCollectionSchema(defaultCollectionID, false),
		}
		loadCollectionTask := &LoadCollectionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadCollectionRequest: req,
			rootCoord:             queryCoord.rootCoordClient,
			dataCoord:             queryCoord.dataCoordClient,
			cluster:               queryCoord.cluster,
			meta:                  queryCoord.meta,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		req := &querypb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleaseCollection,
			},
			CollectionID: defaultCollectionID,
		}
		loadCollectionTask := &ReleaseCollectionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleaseCollectionRequest: req,
			rootCoord:                queryCoord.rootCoordClient,
			cluster:                  queryCoord.cluster,
			meta:                     queryCoord.meta,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test LoadPartition", func(t *testing.T) {
		req := &querypb.LoadPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadPartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		}
		loadCollectionTask := &LoadPartitionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			LoadPartitionsRequest: req,
			dataCoord:             queryCoord.dataCoordClient,
			cluster:               queryCoord.cluster,
			meta:                  queryCoord.meta,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		req := &querypb.ReleasePartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ReleasePartitions,
			},
			CollectionID: defaultCollectionID,
			PartitionIDs: []UniqueID{defaultPartitionID},
		}
		loadCollectionTask := &ReleasePartitionTask{
			BaseTask: BaseTask{
				ctx:              ctx,
				Condition:        NewTaskCondition(ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			ReleasePartitionsRequest: req,
			cluster:                  queryCoord.cluster,
		}

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	err = node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

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
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
)

func setup() {
	Params.Init()
	rand.Seed(time.Now().UnixNano())
	suffix := "-test-query-Coord" + strconv.FormatInt(rand.Int63(), 10)
	Params.MetaRootPath = Params.MetaRootPath + suffix
}

func refreshChannelNames() {
	suffix := "-test-query-Coord" + strconv.FormatInt(rand.Int63n(1000000), 10)
	Params.StatsChannelName = Params.StatsChannelName + suffix
	Params.TimeTickChannelName = Params.TimeTickChannelName + suffix
}

func TestMain(m *testing.M) {
	setup()
	//refreshChannelNames()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestQueryCoord_Init(t *testing.T) {
	ctx := context.Background()
	msFactory := msgstream.NewPmsFactory()
	service, err := NewQueryCoord(context.Background(), msFactory)
	assert.Nil(t, err)
	service.Register()
	service.Init()
	service.Start()

	t.Run("Test Get statistics channel", func(t *testing.T) {
		response, err := service.GetStatisticsChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, response.Value, "query-node-stats")
	})

	t.Run("Test Get timeTick channel", func(t *testing.T) {
		response, err := service.GetTimeTickChannel(ctx)
		assert.Nil(t, err)
		assert.Equal(t, response.Value, "queryTimeTick")
	})

	service.Stop()
}

//func TestQueryCoord_load(t *testing.T) {
//	ctx := context.Background()
//	msFactory := msgstream.NewPmsFactory()
//	service, err := NewQueryCoord(context.Background(), msFactory)
//	assert.Nil(t, err)
//	service.Init()
//	service.Start()
//	service.SetRootCoord(newRootCoordMock())
//	service.SetDataCoord(NewDataMock())
//	registerNodeRequest := &querypb.RegisterNodeRequest{
//		Address: &commonpb.Address{},
//	}
//	service.RegisterNode(ctx, registerNodeRequest)
//
//	t.Run("Test LoadSegment", func(t *testing.T) {
//		loadCollectionRequest := &querypb.LoadCollectionRequest{
//			CollectionID: 1,
//		}
//		response, err := service.LoadCollection(ctx, loadCollectionRequest)
//		assert.Nil(t, err)
//		assert.Equal(t, response.ErrorCode, commonpb.ErrorCode_Success)
//	})
//
//	t.Run("Test LoadPartition", func(t *testing.T) {
//		loadPartitionRequest := &querypb.LoadPartitionsRequest{
//			CollectionID: 1,
//			PartitionIDs: []UniqueID{1},
//		}
//		response, err := service.LoadPartitions(ctx, loadPartitionRequest)
//		assert.Nil(t, err)
//		assert.Equal(t, response.ErrorCode, commonpb.ErrorCode_Success)
//	})
//}

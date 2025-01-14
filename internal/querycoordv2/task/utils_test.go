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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
)

type UtilsSuite struct {
	suite.Suite
}

func (s *UtilsSuite) TestPackLoadSegmentRequest() {
	ctx := context.Background()

	action := NewSegmentAction(1, ActionTypeGrow, "test-ch", 100)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		nil,
		1,
		newReplicaDefaultRG(10),
		action,
	)
	s.NoError(err)

	collectionInfoResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.MmapEnabledKey,
				Value: "false",
			},
		},
	}

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfoResp.GetSchema(),
		collectionInfoResp.GetProperties(),
		&querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		},
		&querypb.SegmentLoadInfo{},
		nil,
	)

	s.True(req.GetNeedTransfer())
	s.Equal(task.CollectionID(), req.CollectionID)
	s.Equal(task.ReplicaID(), req.ReplicaID)
	s.Equal(action.Node(), req.GetDstNodeID())
	for _, field := range req.GetSchema().GetFields() {
		mmapEnable, ok := common.IsMmapDataEnabled(field.GetTypeParams()...)
		s.False(mmapEnable)
		s.True(ok)
	}
}

func (s *UtilsSuite) TestPackLoadSegmentRequestMmap() {
	ctx := context.Background()

	action := NewSegmentAction(1, ActionTypeGrow, "test-ch", 100)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		nil,
		1,
		newReplicaDefaultRG(10),
		action,
	)
	s.NoError(err)

	collectionInfoResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.MmapEnabledKey,
				Value: "true",
			},
		},
	}

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfoResp.GetSchema(),
		collectionInfoResp.GetProperties(),
		&querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		},
		&querypb.SegmentLoadInfo{},
		nil,
	)

	s.True(req.GetNeedTransfer())
	s.Equal(task.CollectionID(), req.CollectionID)
	s.Equal(task.ReplicaID(), req.ReplicaID)
	s.Equal(action.Node(), req.GetDstNodeID())
	for _, field := range req.GetSchema().GetFields() {
		mmapEnable, ok := common.IsMmapDataEnabled(field.GetTypeParams()...)
		s.True(mmapEnable)
		s.True(ok)
	}
}

func TestUtils(t *testing.T) {
	suite.Run(t, new(UtilsSuite))
}

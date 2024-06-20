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

package compaction

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestClusteringCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionTaskSuite))
}

type ClusteringCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *io.MockBinlogIO
	mockAlloc    *allocator.MockAllocator

	task *clusteringCompactionTask

	plan *datapb.CompactionPlan
}

func (s *ClusteringCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *ClusteringCompactionTaskSuite) SetupTest() {
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())
	s.mockAlloc = allocator.NewMockAllocator(s.T())

	s.task = NewClusteringCompactionTask(context.Background(), s.mockBinlogIO, s.mockAlloc, nil)

	paramtable.Get().Save(paramtable.Get().CommonCfg.EntityExpirationTTL.Key, "0")

	s.plan = &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		TimeoutInSeconds: 10,
		Type:             datapb.CompactionType_ClusteringCompaction,
	}
	s.task.plan = s.plan
}

func (s *ClusteringCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *ClusteringCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
}

func (s *ClusteringCompactionTaskSuite) TestWrongCompactionType() {
	s.plan.Type = datapb.CompactionType_MixCompaction
	result, err := s.task.Compact()
	s.Empty(result)
	s.Require().Error(err)
	s.Equal(true, errors.Is(err, merr.ErrIllegalCompactionPlan))
}

func (s *ClusteringCompactionTaskSuite) TestContextDown() {
	ctx, cancel := context.WithCancel(context.Background())
	s.task.ctx = ctx
	cancel()
	result, err := s.task.Compact()
	s.Empty(result)
	s.Require().Error(err)
}

func (s *ClusteringCompactionTaskSuite) TestIsVectorClusteringKey() {
	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = Int32Field
	s.task.init()
	s.Equal(false, s.task.isVectorClusteringKey)
	s.task.plan.ClusteringKeyField = FloatVectorField
	s.task.init()
	s.Equal(true, s.task.isVectorClusteringKey)
}

func (s *ClusteringCompactionTaskSuite) TestGetScalarResult() {
	s.task.plan.Schema = genCollectionSchema()
	s.task.plan.ClusteringKeyField = Int32Field
	_, err := s.task.Compact()
	s.Require().Error(err)
}

func genCollectionSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     "row_id",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  Int32Field,
				Name:     "field_int32",
				DataType: schemapb.DataType_Int32,
			},
			{
				FieldID:  VarCharField,
				Name:     "field_varchar",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:     FloatVectorField,
				Name:        "field_float_vector",
				Description: "float_vector",
				DataType:    schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "4",
					},
				},
			},
		},
	}
}

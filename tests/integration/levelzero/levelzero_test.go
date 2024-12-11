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

package levelzero

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type LevelZeroSuite struct {
	integration.MiniClusterSuite

	schema *schemapb.CollectionSchema
	dim    int
}

func (s *LevelZeroSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableStatsTask.Key, "false")
	s.MiniClusterSuite.SetupSuite()
	s.dim = 768
}

func (s *LevelZeroSuite) TearDownSuite() {
	s.MiniClusterSuite.TearDownSuite()
	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableStatsTask.Key)
}

func TestLevelZero(t *testing.T) {
	suite.Run(t, new(LevelZeroSuite))
}

func (s *LevelZeroSuite) buildCreateCollectionRequest(
	collection string,
	schema *schemapb.CollectionSchema,
	numPartitions int64,
) *milvuspb.CreateCollectionRequest {
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	return &milvuspb.CreateCollectionRequest{
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      1,
		NumPartitions:  numPartitions,
	}
}

func (s *LevelZeroSuite) createCollection(req *milvuspb.CreateCollectionRequest) {
	status, err := s.Cluster.Proxy.CreateCollection(context.TODO(), req)
	s.Require().NoError(err)
	s.Require().True(merr.Ok(status))
	log.Info("CreateCollection result", zap.Any("status", status))
}

// For PrimaryKey field, startPK will be the start PK of this generation
// For PartitionKey field, partitikonKey will be the same in this generation
func (s *LevelZeroSuite) buildFieldDataBySchema(schema *schemapb.CollectionSchema, numRows int, startPK int64, partitionKey int64) []*schemapb.FieldData {
	var fieldData []*schemapb.FieldData
	for _, field := range schema.Fields {
		switch field.DataType {
		case schemapb.DataType_Int64:
			if field.IsPartitionKey {
				fieldData = append(fieldData, integration.NewInt64SameFieldData(field.Name, numRows, partitionKey))
			} else {
				fieldData = append(fieldData, integration.NewInt64FieldDataWithStart(field.Name, numRows, startPK))
			}
		case schemapb.DataType_FloatVector:
			fieldData = append(fieldData, integration.NewFloatVectorFieldData(field.Name, numRows, s.dim))
		default:
			s.Fail("not supported yet")
		}
	}
	return fieldData
}

func (s *LevelZeroSuite) generateSegment(collection string, numRows int, startPk int64, seal bool, partitionKey int64) {
	log.Info("=========================Start generate one segment=========================")
	fieldData := s.buildFieldDataBySchema(s.schema, numRows, startPk, partitionKey)
	hashKeys := integration.GenerateHashKeys(numRows)
	insertResult, err := s.Cluster.Proxy.Insert(context.TODO(), &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     fieldData,
		HashKeys:       hashKeys,
		NumRows:        uint32(numRows),
	})
	s.Require().NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))
	s.Require().EqualValues(numRows, insertResult.GetInsertCnt())
	s.Require().EqualValues(numRows, len(insertResult.GetIDs().GetIntId().GetData()))

	if seal {
		log.Info("=========================Start to flush =========================",
			zap.String("collection", collection),
			zap.Int("numRows", numRows),
			zap.Int64("startPK", startPk),
		)
		s.Flush(collection)
		log.Info("=========================Finish to generate one segment=========================",
			zap.String("collection", collection),
			zap.Int("numRows", numRows),
			zap.Int64("startPK", startPk),
		)
	}
}

func (s *LevelZeroSuite) Flush(collection string) {
	flushResp, err := s.Cluster.Proxy.Flush(context.TODO(), &milvuspb.FlushRequest{
		CollectionNames: []string{collection},
	})
	s.NoError(err)
	segmentLongArr, has := flushResp.GetCollSegIDs()[collection]
	s.Require().True(has)
	segmentIDs := segmentLongArr.GetData() // segmentIDs might be empty
	// s.Require().NotEmpty(segmentLongArr)

	flushTs, has := flushResp.GetCollFlushTs()[collection]
	s.True(has)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.WaitForFlush(ctx, segmentIDs, flushTs, "", collection)
}

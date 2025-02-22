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

package alias

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type AliasSuite struct {
	integration.MiniClusterSuite
}

func (s *AliasSuite) TestAliasOperations() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	// create 2 collection
	const (
		prefix = "TestAliasOperations"
		dim    = 128
		dbName = ""
		rowNum = 3000
	)
	collectionName := prefix + funcutil.GenRandomStr()
	collectionName1 := collectionName + "1"
	collectionName2 := collectionName + "2"

	schema1 := integration.ConstructSchema(collectionName1, dim, true)
	marshaledSchema1, err := proto.Marshal(schema1)
	s.NoError(err)
	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName1,
		Schema:         marshaledSchema1,
	})
	s.NoError(err)
	log.Info("CreateCollection 1 result", zap.Any("createCollectionStatus", createCollectionStatus))

	schema2 := integration.ConstructSchema(collectionName2, dim, true)
	marshaledSchema2, err := proto.Marshal(schema2)
	s.NoError(err)
	createCollectionStatus2, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName2,
		Schema:         marshaledSchema2,
	})
	s.NoError(err)
	log.Info("CreateCollection 2 result", zap.Any("createCollectionStatus", createCollectionStatus2))

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName1,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	insertResult2, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName2,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult2.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName1},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName1]
	ids := segmentIDs.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName1]
	s.Require().True(has)
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName1)

	flushResp2, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName2},
	})
	s.NoError(err)
	segmentIDs2, has2 := flushResp2.GetCollSegIDs()[collectionName2]
	ids2 := segmentIDs2.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has2)
	flushTs2, has2 := flushResp2.GetCollFlushTs()[collectionName2]
	s.Require().True(has2)
	s.WaitForFlush(ctx, ids2, flushTs2, dbName, collectionName2)

	// create alias
	// alias11 -> collection1
	// alias12 -> collection1
	// alias21 -> collection2
	createAliasResp1, err := c.Proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		CollectionName: collectionName1,
		Alias:          "alias11",
	})
	s.NoError(err)
	s.Equal(createAliasResp1.GetErrorCode(), commonpb.ErrorCode_Success)
	createAliasResp2, err := c.Proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		CollectionName: collectionName1,
		Alias:          "alias12",
	})
	s.NoError(err)
	s.Equal(createAliasResp2.GetErrorCode(), commonpb.ErrorCode_Success)
	createAliasResp3, err := c.Proxy.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		CollectionName: collectionName2,
		Alias:          "alias21",
	})
	s.NoError(err)
	s.Equal(createAliasResp3.GetErrorCode(), commonpb.ErrorCode_Success)

	describeAliasResp1, err := c.Proxy.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{
		Alias: "alias11",
	})
	s.NoError(err)
	s.Equal(describeAliasResp1.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	s.Equal(collectionName1, describeAliasResp1.GetCollection())
	log.Info("describeAliasResp1",
		zap.String("alias", describeAliasResp1.GetAlias()),
		zap.String("collection", describeAliasResp1.GetCollection()))

	describeAliasResp2, err := c.Proxy.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{
		Alias: "alias12",
	})
	s.NoError(err)
	s.Equal(describeAliasResp2.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	s.Equal(collectionName1, describeAliasResp2.GetCollection())
	log.Info("describeAliasResp2",
		zap.String("alias", describeAliasResp2.GetAlias()),
		zap.String("collection", describeAliasResp2.GetCollection()))

	describeAliasResp3, err := c.Proxy.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{
		Alias: "alias21",
	})
	s.NoError(err)
	s.Equal(describeAliasResp3.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	s.Equal(collectionName2, describeAliasResp3.GetCollection())
	log.Info("describeAliasResp3",
		zap.String("alias", describeAliasResp3.GetAlias()),
		zap.String("collection", describeAliasResp3.GetCollection()))

	listAliasesResp, err := c.Proxy.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
	s.NoError(err)
	s.Equal(listAliasesResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	s.Equal(3, len(listAliasesResp.Aliases))

	log.Info("listAliasesResp", zap.Strings("aliases", listAliasesResp.Aliases))

	dropAliasResp1, err := c.Proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{
		Alias: "alias11",
	})
	s.NoError(err)
	s.Equal(dropAliasResp1.GetErrorCode(), commonpb.ErrorCode_Success)

	dropAliasResp3, err := c.Proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{
		Alias: "alias21",
	})
	s.NoError(err)
	s.Equal(dropAliasResp3.GetErrorCode(), commonpb.ErrorCode_Success)

	listAliasesRespNew, err := c.Proxy.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
	s.NoError(err)
	s.Equal(listAliasesRespNew.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	s.Equal(1, len(listAliasesRespNew.Aliases))
	log.Info("listAliasesResp after drop", zap.Strings("aliases", listAliasesResp.Aliases))

	log.Info("======================")
	log.Info("======================")
	log.Info("TestAliasOperations succeed")
	log.Info("======================")
	log.Info("======================")
}

func TestAliasOperations(t *testing.T) {
	suite.Run(t, new(AliasSuite))
}

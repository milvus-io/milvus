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

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type AliasSuite struct {
	integration.MiniClusterSuite
}

func (s *AliasSuite) TestAliasOperations() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()
	c := s.Cluster

	prefix := "TestAliasOperations"

	// create 2 collection
	dbName := ""
	dim := 128
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
		ShardsNum:      2,
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
		ShardsNum:      2,
	})
	s.NoError(err)
	log.Info("CreateCollection 2 result", zap.Any("createCollectionStatus", createCollectionStatus2))

	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	s.Equal(2, len(showCollectionsResp.CollectionNames))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

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

	log.Info("TestAliasOperations succeed")
}

func TestAliasOperations(t *testing.T) {
	suite.Run(t, new(AliasSuite))
}

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

package refreshconfig

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type RefreshConfigSuite struct {
	integration.MiniClusterSuite
}

func (s *RefreshConfigSuite) TestRefreshPasswordLength() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	resp, err := c.Proxy.CreateCredential(ctx, &milvuspb.CreateCredentialRequest{
		Username: "test",
		Password: "1234",
	})
	log.Debug("first create result", zap.Any("state", resp))
	s.Require().NoError(err)
	s.Equal(commonpb.ErrorCode_IllegalArgument, resp.GetErrorCode())

	params := paramtable.Get()
	key := fmt.Sprintf("%s/config/proxy/minpasswordlength", params.EtcdCfg.RootPath.GetValue())
	c.EtcdCli.KV.Put(ctx, key, "3")

	s.Eventually(func() bool {
		resp, err = c.Proxy.CreateCredential(ctx, &milvuspb.CreateCredentialRequest{
			Username: "test",
			Password: "1234",
		})
		log.Debug("second create result", zap.Any("state", resp))
		return commonpb.ErrorCode_Success == resp.GetErrorCode()
	}, time.Second*20, time.Millisecond*500)

}

func (s *RefreshConfigSuite) TestRefreshDefaultIndexName() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()
	params := paramtable.Get()
	c.EtcdCli.KV.Put(ctx, fmt.Sprintf("%s/config/common/defaultIndexName", params.EtcdCfg.RootPath.GetValue()), "a_index")

	s.Eventually(func() bool {
		return params.CommonCfg.DefaultIndexName.GetValue() == "a_index"
	}, time.Second*10, time.Millisecond*500)

	dim := 128
	dbName := "default"
	collectionName := "test"
	rowNum := 100

	schema := integration.ConstructSchema("test", 128, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test",
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	_, err = c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)

	// flush
	flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	s.True(has)
	ids := segmentIDs.GetData()
	s.NotEmpty(segmentIDs)

	s.WaitForFlush(ctx, ids)

	_, err = c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, distance.L2),
	})
	s.NoError(err)

	resp, err := c.Proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, resp.Status.GetErrorCode())
	s.Equal(1, len(resp.IndexDescriptions))
	s.Equal("a_index_101", resp.IndexDescriptions[0].GetIndexName())
}

func TestRefreshConfig(t *testing.T) {
	suite.Run(t, new(RefreshConfigSuite))
}

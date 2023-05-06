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

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRefreshPasswordLength(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)

	err = c.Start()
	assert.NoError(t, err)
	defer func() {
		err = c.Stop()
		assert.NoError(t, err)
		cancel()
	}()

	s, err := c.proxy.CreateCredential(ctx, &milvuspb.CreateCredentialRequest{
		Username: "test",
		Password: "1234",
	})
	log.Debug("first create result", zap.Any("state", s))
	assert.Equal(t, commonpb.ErrorCode_IllegalArgument, s.GetErrorCode())

	params := paramtable.Get()
	c.etcdCli.KV.Put(ctx, fmt.Sprintf("%s/config/proxy/minpasswordlength", params.EtcdCfg.RootPath.GetValue()), "3")

	assert.Eventually(t, func() bool {
		s, err = c.proxy.CreateCredential(ctx, &milvuspb.CreateCredentialRequest{
			Username: "test",
			Password: "1234",
		})
		log.Debug("second create result", zap.Any("state", s))
		return commonpb.ErrorCode_Success == s.GetErrorCode()
	}, time.Second*20, time.Millisecond*500)
}

func TestRefreshDefaultIndexName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)

	err = c.Start()
	assert.NoError(t, err)
	defer func() {
		err = c.Stop()
		assert.NoError(t, err)
		cancel()
	}()

	params := paramtable.Get()
	c.etcdCli.KV.Put(ctx, fmt.Sprintf("%s/config/common/defaultIndexName", params.EtcdCfg.RootPath.GetValue()), "a_index")

	assert.Eventually(t, func() bool {
		return params.CommonCfg.DefaultIndexName.GetValue() == "a_index"
	}, time.Second*10, time.Millisecond*500)

	dim := 128
	dbName := "default"
	collectionName := "test"
	rowNum := 100

	schema := constructSchema("test", 128, true)
	marshaledSchema, err := proto.Marshal(schema)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test",
		Schema:         marshaledSchema,
		ShardsNum:      1,
	})
	assert.NoError(t, err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	hashKeys := generateHashKeys(rowNum)
	_, err = c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)

	_, err = c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      floatVecField,
		ExtraParams:    constructIndexParam(dim, IndexFaissIvfFlat, distance.L2),
	})

	s, err := c.proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.Equal(t, commonpb.ErrorCode_Success, s.Status.GetErrorCode())
	assert.Equal(t, 1, len(s.IndexDescriptions))
	assert.Equal(t, "a_index_101", s.IndexDescriptions[0].GetIndexName())
}

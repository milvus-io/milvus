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

package cluster

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type ClusterClient interface {
	CreateMilvusClient(ctx context.Context, cluster *commonpb.MilvusCluster) (MilvusClient, error)
}

var _ ClusterClient = (*clusterClient)(nil)

type clusterClient struct{}

func NewClusterClient() ClusterClient {
	return &clusterClient{}
}

func (c *clusterClient) CreateMilvusClient(ctx context.Context, cluster *commonpb.MilvusCluster) (MilvusClient, error) {
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: cluster.GetConnectionParam().GetUri(),
		APIKey:  cluster.GetConnectionParam().GetToken(),
	})
	if err != nil {
		log.Warn("failed to create milvus client", zap.Error(err))
		return nil, err
	}
	return cli, nil
}

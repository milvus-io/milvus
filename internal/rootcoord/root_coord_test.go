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

package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

func TestRootCoord_CreateDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_ListDatabases(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_CreateCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CreatePartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropPartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CreateAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_AlterAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DescribeCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_HasCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowCollections(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_HasPartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowPartitions(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_AllocTimestamp(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate ts", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidTsoAllocator())
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		alloc := newMockTsoAllocator()
		count := uint32(10)
		ts := Timestamp(100)
		alloc.GenerateTSOF = func(count uint32) (uint64, error) {
			// end ts
			return ts, nil
		}
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTsoAllocator(alloc))
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{Count: count})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		// begin ts
		assert.Equal(t, ts-uint64(count)+1, resp.GetTimestamp())
		assert.Equal(t, count, resp.GetCount())
	})
}

func TestRootCoord_AllocID(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate id", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidIDAllocator())
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		alloc := newMockIDAllocator()
		id := UniqueID(100)
		alloc.AllocF = func(count uint32) (UniqueID, UniqueID, error) {
			return id, id + int64(count), nil
		}
		count := uint32(10)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withIDAllocator(alloc))
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{Count: count})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, id, resp.GetID())
		assert.Equal(t, count, resp.GetCount())
	})
}

func TestRootCoord_UpdateChannelTimeTick(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("invalid msg type", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("invalid msg", func(t *testing.T) {
		defer cleanTestEnv()

		ticker := newRocksMqTtSynchronizer()

		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTtSynchronizer(ticker))

		// the length of channel names & timestamps mismatch.
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_TimeTick,
			},
			ChannelNames: []string{funcutil.GenRandomStr()},
			Timestamps:   []uint64{},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		source := int64(20220824)
		ts := Timestamp(100)
		defaultTs := Timestamp(101)

		ticker := newRocksMqTtSynchronizer()
		ticker.addSession(&sessionutil.Session{ServerID: source})

		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTtSynchronizer(ticker))

		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				SourceID: source,
				MsgType:  commonpb.MsgType_TimeTick,
			},
			ChannelNames:     []string{funcutil.GenRandomStr()},
			Timestamps:       []uint64{ts},
			DefaultTimestamp: defaultTs,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_InvalidateCollectionMetaCache(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to invalidate cache", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidProxyManager())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withValidProxyManager())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_RenameCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("add task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_ShowConfigurations(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		Params.Init()

		pattern := "rootcoord.Port"
		req := &internalpb.ShowConfigurationsRequest{
			Base: &commonpb.MsgBase{
				MsgID: rand.Int63(),
			},
			Pattern: pattern,
		}

		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 1, len(resp.GetConfiguations()))
		assert.Equal(t, "rootcoord.port", resp.GetConfiguations()[0].Key)
	})
}

func TestRootCoord_GetMetrics(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to parse metric type", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "invalid request",
		}
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("unsupported metric type", func(t *testing.T) {
		// unsupported metric type
		unsupportedMetricType := "unsupported"
		req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get system info metrics from cache", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		c.metricsCacheManager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{
			Status:        succStatus(),
			Response:      "cached response",
			ComponentName: "cached component",
		})
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get system info metrics, cache miss", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		c.metricsCacheManager.InvalidateSystemInfoMetrics()
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get system info metrics", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		resp, err := c.getSystemInfoMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})
}

func TestCore_Import(t *testing.T) {
	meta := newMockMetaTable()
	meta.AddCollectionFunc = func(ctx context.Context, coll *model.Collection) error {
		return nil
	}
	meta.ChangeCollectionStateFunc = func(ctx context.Context, collectionID UniqueID, state etcdpb.CollectionState, ts Timestamp) error {
		return nil
	}

	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.Import(ctx, &milvuspb.ImportRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("bad collection name", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMeta(meta))
		meta.GetCollectionIDByNameFunc = func(name string) (UniqueID, error) {
			return 0, errors.New("error mock GetCollectionIDByName")
		}
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return nil, errors.New("collection name not found")
		}
		_, err := c.Import(ctx, &milvuspb.ImportRequest{
			CollectionName: "a-bad-name",
		})
		assert.Error(t, err)
	})

	t.Run("bad partition name", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMeta(meta))
		meta.GetCollectionIDByNameFunc = func(name string) (UniqueID, error) {
			return 100, nil
		}
		meta.GetCollectionVirtualChannelsFunc = func(colID int64) []string {
			return []string{"ch-1", "ch-2"}
		}
		meta.GetPartitionByNameFunc = func(collID UniqueID, partitionName string, ts Timestamp) (UniqueID, error) {
			return 0, errors.New("mock GetPartitionByNameFunc error")
		}
		_, err := c.Import(ctx, &milvuspb.ImportRequest{
			CollectionName: "a-good-name",
		})
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMeta(meta))
		meta.GetCollectionIDByNameFunc = func(name string) (UniqueID, error) {
			return 100, nil
		}
		meta.GetCollectionVirtualChannelsFunc = func(colID int64) []string {
			return []string{"ch-1", "ch-2"}
		}
		meta.GetPartitionByNameFunc = func(collID UniqueID, partitionName string, ts Timestamp) (UniqueID, error) {
			return 101, nil
		}
		coll := &model.Collection{Name: "a-good-name"}
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		_, err := c.Import(ctx, &milvuspb.ImportRequest{
			CollectionName: "a-good-name",
		})
		assert.NoError(t, err)
	})

	t.Run("backup without partition name", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMeta(meta))

		coll := &model.Collection{
			Name: "a-good-name",
		}
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		resp, _ := c.Import(ctx, &milvuspb.ImportRequest{
			CollectionName: "a-good-name",
			Options: []*commonpb.KeyValuePair{
				{Key: importutil.BackupFlag, Value: "true"},
			},
		})
		assert.NotNil(t, resp)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	// Remove the following case after bulkinsert can support partition key
	t.Run("unsupport partition key", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMeta(meta))

		coll := &model.Collection{
			Name: "a-good-name",
			Fields: []*model.Field{
				{IsPartitionKey: true},
			},
		}
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		resp, _ := c.Import(ctx, &milvuspb.ImportRequest{
			CollectionName: "a-good-name",
		})
		assert.NotNil(t, resp)
	})
}

func TestCore_GetImportState(t *testing.T) {
	mockKv := memkv.NewMemoryKV()
	ti1 := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	ti2 := &datapb.ImportTaskInfo{
		Id: 200,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	mockKv.Save(BuildImportTaskKey(1), "value")
	mockKv.Save(BuildImportTaskKey(100), string(taskInfo1))
	mockKv.Save(BuildImportTaskKey(200), string(taskInfo2))

	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.GetImportState(ctx, &milvuspb.GetImportStateRequest{
			Task: 100,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		c.importManager = newImportManager(ctx, mockKv, nil, nil, nil, nil, nil)
		resp, err := c.GetImportState(ctx, &milvuspb.GetImportStateRequest{
			Task: 100,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(100), resp.GetId())
		assert.NotEqual(t, 0, resp.GetCreateTs())
		assert.Equal(t, commonpb.ImportState_ImportPending, resp.GetState())
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestCore_ListImportTasks(t *testing.T) {
	mockKv := memkv.NewMemoryKV()
	ti1 := &datapb.ImportTaskInfo{
		Id:             100,
		CollectionName: "collection-A",
		CollectionId:   1,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
		CreateTs: time.Now().Unix() - 300,
	}
	ti2 := &datapb.ImportTaskInfo{
		Id:             200,
		CollectionName: "collection-A",
		CollectionId:   1,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 200,
	}
	ti3 := &datapb.ImportTaskInfo{
		Id:             300,
		CollectionName: "collection-B",
		CollectionId:   2,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	taskInfo3, err := proto.Marshal(ti3)
	assert.NoError(t, err)
	mockKv.Save(BuildImportTaskKey(1), "value") // this item will trigger an error log in importManager.loadFromTaskStore()
	mockKv.Save(BuildImportTaskKey(100), string(taskInfo1))
	mockKv.Save(BuildImportTaskKey(200), string(taskInfo2))
	mockKv.Save(BuildImportTaskKey(300), string(taskInfo3))

	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	verifyTaskFunc := func(task *milvuspb.GetImportStateResponse, taskID int64, colID int64, state commonpb.ImportState) {
		assert.Equal(t, commonpb.ErrorCode_Success, task.GetStatus().ErrorCode)
		assert.Equal(t, taskID, task.GetId())
		assert.Equal(t, state, task.GetState())
		assert.Equal(t, colID, task.GetCollectionId())
	}

	t.Run("normal case", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			if collectionName == ti1.CollectionName {
				return &model.Collection{
					CollectionID: ti1.CollectionId,
				}, nil
			} else if collectionName == ti3.CollectionName {
				return &model.Collection{
					CollectionID: ti3.CollectionId,
				}, nil
			}
			return nil, errors.New("GetCollectionByName error")
		}

		ctx := context.Background()
		c := newTestCore(withHealthyCode(), withMeta(meta))
		c.importManager = newImportManager(ctx, mockKv, nil, nil, nil, nil, nil)

		// list all tasks
		resp, err := c.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(resp.GetTasks()))
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		verifyTaskFunc(resp.GetTasks()[0], 100, 1, commonpb.ImportState_ImportPending)
		verifyTaskFunc(resp.GetTasks()[1], 200, 1, commonpb.ImportState_ImportPersisted)
		verifyTaskFunc(resp.GetTasks()[2], 300, 2, commonpb.ImportState_ImportPersisted)

		// list tasks of collection-A
		resp, err = c.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{
			CollectionName: "collection-A",
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(resp.GetTasks()))
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// list tasks of collection-B
		resp, err = c.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{
			CollectionName: "collection-B",
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.GetTasks()))
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// invalid collection name
		resp, err = c.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{
			CollectionName: "dummy",
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(resp.GetTasks()))
		assert.Equal(t, commonpb.ErrorCode_IllegalCollectionName, resp.GetStatus().GetErrorCode())

		// list the latest 2 tasks
		resp, err = c.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{
			Limit: 2,
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(resp.GetTasks()))
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		verifyTaskFunc(resp.GetTasks()[0], 200, 1, commonpb.ImportState_ImportPersisted)
		verifyTaskFunc(resp.GetTasks()[1], 300, 2, commonpb.ImportState_ImportPersisted)

		// failed to load tasks from store
		mockTxnKV := &mocks.TxnKV{}
		mockTxnKV.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("mock error"))
		c.importManager.taskStore = mockTxnKV
		resp, err = c.ListImportTasks(ctx, &milvuspb.ListImportTasksRequest{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(resp.GetTasks()))
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})
}

func TestCore_ReportImport(t *testing.T) {
	paramtable.Get().Save(Params.RootCoordCfg.ImportTaskSubPath.Key, "importtask")
	var countLock sync.RWMutex
	var globalCount = typeutil.UniqueID(0)
	var idAlloc = func(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
		countLock.Lock()
		defer countLock.Unlock()
		globalCount++
		return globalCount, 0, nil
	}
	mockKv := memkv.NewMemoryKV()
	ti1 := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	ti2 := &datapb.ImportTaskInfo{
		Id: 200,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
		CreateTs: time.Now().Unix() - 100,
	}
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	mockKv.Save(BuildImportTaskKey(1), "value")
	mockKv.Save(BuildImportTaskKey(100), string(taskInfo1))
	mockKv.Save(BuildImportTaskKey(200), string(taskInfo2))

	ticker := newRocksMqTtSynchronizer()
	meta := newMockMetaTable()
	meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
		return nil, errors.New("error mock GetCollectionByName")
	}
	meta.AddCollectionFunc = func(ctx context.Context, coll *model.Collection) error {
		return nil
	}
	meta.ChangeCollectionStateFunc = func(ctx context.Context, collectionID UniqueID, state etcdpb.CollectionState, ts Timestamp) error {
		return nil
	}

	dc := newMockDataCoord()
	dc.GetComponentStatesFunc = func(ctx context.Context) (*milvuspb.ComponentStates, error) {
		return &milvuspb.ComponentStates{
			State: &milvuspb.ComponentInfo{
				NodeID:    TestRootCoordID,
				StateCode: commonpb.StateCode_Healthy,
			},
			SubcomponentStates: nil,
			Status:             succStatus(),
		}, nil
	}
	dc.WatchChannelsFunc = func(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
		return &datapb.WatchChannelsResponse{Status: succStatus()}, nil
	}
	dc.FlushFunc = func(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
		return &datapb.FlushResponse{Status: succStatus()}, nil
	}

	mockCallImportServiceErr := false
	callImportServiceFn := func(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
		if mockCallImportServiceErr {
			return &datapb.ImportTaskResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
			}, errors.New("mock err")
		}
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	callGetSegmentStates := func(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}, nil
	}

	callUnsetIsImportingState := func(context.Context, *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.ReportImport(ctx, &rootcoordpb.ImportResult{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("report complete import with task not found", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		c.importManager = newImportManager(ctx, mockKv, idAlloc, callImportServiceFn, callGetSegmentStates, nil, nil)
		resp, err := c.ReportImport(ctx, &rootcoordpb.ImportResult{
			TaskId: 101,
			State:  commonpb.ImportState_ImportCompleted,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	testFunc := func(state commonpb.ImportState) {
		ctx := context.Background()
		c := newTestCore(
			withHealthyCode(),
			withValidIDAllocator(),
			withMeta(meta),
			withTtSynchronizer(ticker),
			withDataCoord(dc))
		c.broker = newServerBroker(c)
		c.importManager = newImportManager(ctx, mockKv, idAlloc, callImportServiceFn, callGetSegmentStates, nil, callUnsetIsImportingState)
		c.importManager.loadFromTaskStore(true)
		c.importManager.sendOutTasks(ctx)

		resp, err := c.ReportImport(ctx, &rootcoordpb.ImportResult{
			TaskId: 100,
			State:  state,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		// Change the state back.
		err = c.importManager.setImportTaskState(100, commonpb.ImportState_ImportPending)
		assert.NoError(t, err)
	}

	t.Run("report import started state", func(t *testing.T) {
		testFunc(commonpb.ImportState_ImportStarted)
	})

	t.Run("report import persisted state", func(t *testing.T) {
		testFunc(commonpb.ImportState_ImportPersisted)
	})

	t.Run("report import completed state", func(t *testing.T) {
		testFunc(commonpb.ImportState_ImportCompleted)
	})

	t.Run("report import failed state", func(t *testing.T) {
		testFunc(commonpb.ImportState_ImportFailed)
	})
}

func TestCore_Rbac(t *testing.T) {
	ctx := context.Background()
	c := &Core{
		ctx: ctx,
	}

	// not healthy.
	c.stateCode.Store(commonpb.StateCode_Abnormal)

	{
		resp, err := c.CreateRole(ctx, &milvuspb.CreateRoleRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.DropRole(ctx, &milvuspb.DropRoleRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.SelectRole(ctx, &milvuspb.SelectRoleRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	}

	{
		resp, err := c.SelectUser(ctx, &milvuspb.SelectUserRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	}

	{
		resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	}

	{
		resp, err := c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	}
}

func TestCore_sendMinDdlTsAsTt(t *testing.T) {
	ticker := newRocksMqTtSynchronizer()
	ddlManager := newMockDdlTsLockManager()
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	sched := newMockScheduler()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	c := newTestCore(
		withTtSynchronizer(ticker),
		withDdlTsLockManager(ddlManager),
		withScheduler(sched))

	c.stateCode.Store(commonpb.StateCode_Healthy)
	c.session.ServerID = TestRootCoordID
	c.sendMinDdlTsAsTt() // no session.
	ticker.addSession(&sessionutil.Session{ServerID: TestRootCoordID})
	c.sendMinDdlTsAsTt()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.ZeroTimestamp
	}
	c.sendMinDdlTsAsTt() // zero ts
	sched.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.MaxTimestamp
	}
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.MaxTimestamp
	}
	c.sendMinDdlTsAsTt()
}

func TestCore_startTimeTickLoop(t *testing.T) {
	ticker := newRocksMqTtSynchronizer()
	ticker.addSession(&sessionutil.Session{ServerID: TestRootCoordID})
	ddlManager := newMockDdlTsLockManager()
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	sched := newMockScheduler()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	c := newTestCore(
		withTtSynchronizer(ticker),
		withDdlTsLockManager(ddlManager),
		withScheduler(sched))
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	paramtable.Get().Save(Params.ProxyCfg.TimeTickInterval.Key, "1")
	c.wg.Add(1)
	c.UpdateStateCode(commonpb.StateCode_Initializing)
	go c.startTimeTickLoop()

	time.Sleep(time.Millisecond * 4)
	cancel()
	c.wg.Wait()
}

// make sure the main functions work well when EnableActiveStandby=true
func TestRootcoord_EnableActiveStandby(t *testing.T) {
	randVal := rand.Int()
	Params.Init()
	Params.BaseTable.Save("etcd.rootPath", fmt.Sprintf("/%d", randVal))
	paramtable.Get().Save(Params.RootCoordCfg.EnableActiveStandby.Key, "true")
	paramtable.Get().Save(Params.CommonCfg.RootCoordTimeTick.Key, fmt.Sprintf("rootcoord-time-tick-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordStatistics.Key, fmt.Sprintf("rootcoord-statistics-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordSubName.Key, fmt.Sprintf("subname-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, fmt.Sprintf("rootcoord-dml-test-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordDelta.Key, fmt.Sprintf("rootcoord-delta-test-%d", randVal))

	ctx := context.Background()
	coreFactory := dependency.NewDefaultFactory(true)
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	core, err := NewCore(ctx, coreFactory)
	core.etcdCli = etcdCli
	assert.NoError(t, err)
	err = core.Init()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_StandBy, core.stateCode.Load().(commonpb.StateCode))
	err = core.Start()
	assert.NoError(t, err)
	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, core.stateCode.Load().(commonpb.StateCode))
	resp, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		CollectionName: "unexist"})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	err = core.Stop()
	assert.NoError(t, err)
}

// make sure the main functions work well when EnableActiveStandby=false
func TestRootcoord_DisableActiveStandby(t *testing.T) {
	randVal := rand.Int()
	Params.Init()
	Params.BaseTable.Save("etcd.rootPath", fmt.Sprintf("/%d", randVal))
	paramtable.Get().Save(Params.RootCoordCfg.EnableActiveStandby.Key, "false")
	paramtable.Get().Save(Params.CommonCfg.RootCoordTimeTick.Key, fmt.Sprintf("rootcoord-time-tick-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordStatistics.Key, fmt.Sprintf("rootcoord-statistics-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordSubName.Key, fmt.Sprintf("subname-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, fmt.Sprintf("rootcoord-dml-test-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordDelta.Key, fmt.Sprintf("rootcoord-delta-test-%d", randVal))

	ctx := context.Background()
	coreFactory := dependency.NewDefaultFactory(true)
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	core, err := NewCore(ctx, coreFactory)
	core.etcdCli = etcdCli
	assert.NoError(t, err)
	err = core.Init()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Initializing, core.stateCode.Load().(commonpb.StateCode))
	err = core.Start()
	assert.NoError(t, err)
	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, core.stateCode.Load().(commonpb.StateCode))
	resp, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		CollectionName: "unexist"})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	err = core.Stop()
	assert.NoError(t, err)
}

func TestRootCoord_AlterCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("add task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CheckHealth(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("proxy health check is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidProxyManager())

		ctx := context.Background()
		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})

	t.Run("proxy health check is fail", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidProxyManager())

		ctx := context.Background()
		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})
}

func TestCore_Stop(t *testing.T) {
	t.Run("abnormal stop before component is ready", func(t *testing.T) {
		c := &Core{}
		err := c.Stop()
		assert.NoError(t, err)
		code, ok := c.stateCode.Load().(commonpb.StateCode)
		assert.True(t, ok)
		assert.Equal(t, commonpb.StateCode_Abnormal, code)
	})

	t.Run("normal case", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		c.ctx, c.cancel = context.WithCancel(context.Background())
		err := c.Stop()
		assert.NoError(t, err)
		code, ok := c.stateCode.Load().(commonpb.StateCode)
		assert.True(t, ok)
		assert.Equal(t, commonpb.StateCode_Abnormal, code)
	})
}

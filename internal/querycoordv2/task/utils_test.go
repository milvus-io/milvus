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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/common"
)

func Test_getMetricType(t *testing.T) {
	ctx := context.Background()
	collection := int64(1)
	schema := &schemapb.CollectionSchema{
		Name: "TestGetMetricType",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}
	indexInfo := &indexpb.IndexInfo{
		CollectionID: collection,
		FieldID:      100,
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MetricTypeKey,
				Value: "L2",
			},
		},
	}
	t.Run("test normal", func(t *testing.T) {
		broker := meta.NewMockBroker(t)
		broker.EXPECT().DescribeIndex(mock.Anything, collection).
			Return([]*indexpb.IndexInfo{indexInfo}, nil)
		metricType, err := getMetricType(ctx, collection, schema, broker)
		assert.NoError(t, err)
		assert.Equal(t, "L2", metricType)
	})
	t.Run("test describe index failed", func(t *testing.T) {
		broker := meta.NewMockBroker(t)
		broker.EXPECT().DescribeIndex(mock.Anything, collection).
			Return(nil, fmt.Errorf("mock err"))
		_, err := getMetricType(ctx, collection, schema, broker)
		assert.Error(t, err)
	})
	t.Run("test get vec field failed", func(t *testing.T) {
		broker := meta.NewMockBroker(t)
		broker.EXPECT().DescribeIndex(mock.Anything, collection).
			Return([]*indexpb.IndexInfo{indexInfo}, nil)
		_, err := getMetricType(ctx, collection, &schemapb.CollectionSchema{
			Name: "TestGetMetricType",
		}, broker)
		assert.Error(t, err)
	})
	t.Run("test field id mismatch", func(t *testing.T) {
		broker := meta.NewMockBroker(t)
		broker.EXPECT().DescribeIndex(mock.Anything, collection).
			Return([]*indexpb.IndexInfo{indexInfo}, nil)
		_, err := getMetricType(ctx, collection, &schemapb.CollectionSchema{
			Name: "TestGetMetricType",
			Fields: []*schemapb.FieldSchema{
				{FieldID: -1, Name: "vec", DataType: schemapb.DataType_FloatVector},
			},
		}, broker)
		assert.Error(t, err)
	})
	t.Run("test no metric type", func(t *testing.T) {
		broker := meta.NewMockBroker(t)
		broker.EXPECT().DescribeIndex(mock.Anything, collection).
			Return([]*indexpb.IndexInfo{{
				CollectionID: collection,
				FieldID:      100,
			}}, nil)
		_, err := getMetricType(ctx, collection, schema, broker)
		assert.Error(t, err)
	})
}

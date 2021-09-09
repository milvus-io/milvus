// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/indexnode"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/stretchr/testify/assert"
)

func TestGetSystemInfoMetrics(t *testing.T) {
	ctx := context.Background()
	ic, err := NewIndexCoord(ctx)
	assert.Nil(t, err)
	Params.Init()
	err = ic.Register()
	assert.Nil(t, err)

	err = ic.Init()
	assert.Nil(t, err)
	err = ic.Start()
	assert.Nil(t, err)

	t.Run("getSystemInfoMetrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.Nil(t, err)

		resp, err := getSystemInfoMetrics(ctx, req, ic)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("getSystemInfoMetrics error", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.Nil(t, err)

		inm1 := &indexnode.Mock{
			Failure: true,
			Err:     true,
		}
		inm2 := &indexnode.Mock{
			Failure: true,
			Err:     false,
		}

		ic.nodeManager.setClient(1, inm1)
		ic.nodeManager.setClient(2, inm2)

		resp, err := getSystemInfoMetrics(ctx, req, ic)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
	})

	err = ic.Stop()
	assert.Nil(t, err)
}

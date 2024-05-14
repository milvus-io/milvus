// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package balance

import (
	"reflect"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestGetBalancer(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()

	balancer := GetBalancer(nil, nil, nil, nil, nil)
	assert.Equal(t, reflect.TypeOf(balancer), reflect.TypeOf(new(ScoreBasedBalancer)))

	params.Save(params.QueryCoordCfg.Balancer.Key, meta.ChannelLevelScoreBalancerName)
	balancer = GetBalancer(nil, nil, nil, nil, nil)
	assert.Equal(t, reflect.TypeOf(balancer).String(), reflect.TypeOf(new(ChannelLevelScoreBalancer)).String())

	params.Save(params.QueryCoordCfg.Balancer.Key, meta.ScoreBasedBalancerName)
	balancer = GetBalancer(nil, nil, nil, nil, nil)
	assert.Equal(t, reflect.TypeOf(balancer).String(), reflect.TypeOf(new(ScoreBasedBalancer)).String())

	params.Save(params.QueryCoordCfg.Balancer.Key, meta.MultiTargetBalancerName)
	balancer = GetBalancer(nil, nil, nil, nil, nil)
	assert.Equal(t, reflect.TypeOf(balancer).String(), reflect.TypeOf(new(MultiTargetBalancer)).String())

	params.Save(params.QueryCoordCfg.Balancer.Key, meta.RoundRobinBalancerName)
	balancer = GetBalancer(nil, nil, nil, nil, nil)
	assert.Equal(t, reflect.TypeOf(balancer).String(), reflect.TypeOf(new(RoundRobinBalancer)).String())

	params.Save(params.QueryCoordCfg.Balancer.Key, meta.RowCountBasedBalancerName)
	balancer = GetBalancer(nil, nil, nil, nil, nil)
	assert.Equal(t, reflect.TypeOf(balancer).String(), reflect.TypeOf(new(RowCountBasedBalancer)).String())
	log.Info("xxx", zap.Any("xxx", reflect.TypeOf(balancer).String()), zap.Any("xxx", reflect.TypeOf(new(ScoreBasedBalancer)).String()))
}

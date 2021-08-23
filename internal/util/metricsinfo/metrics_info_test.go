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

package metricsinfo

import (
	"testing"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestBaseComponentInfos_Codec(t *testing.T) {
	infos1 := BaseComponentInfos{
		Name: ConstructComponentName(typeutil.ProxyRole, 1),
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestBaseComponentInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 BaseComponentInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1.Name, infos2.Name)
}

func TestQueryNodeInfos_Codec(t *testing.T) {
	infos1 := QueryNodeInfos{
		BaseComponentInfos: BaseComponentInfos{
			Name: ConstructComponentName(typeutil.QueryNodeRole, 1),
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestQueryNodeInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 QueryNodeInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1.Name, infos2.Name)
}

func TestQueryCoordInfos_Codec(t *testing.T) {
	infos1 := QueryCoordInfos{
		BaseComponentInfos: BaseComponentInfos{
			Name: ConstructComponentName(typeutil.QueryCoordRole, 1),
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestQueryCoordInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 QueryCoordInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1.Name, infos2.Name)
}

func TestIndexNodeInfos_Codec(t *testing.T) {
	infos1 := IndexNodeInfos{
		BaseComponentInfos: BaseComponentInfos{
			Name: ConstructComponentName(typeutil.IndexNodeRole, 1),
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestIndexNodeInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 IndexNodeInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1.Name, infos2.Name)
}

func TestIndexCoordInfos_Codec(t *testing.T) {
	infos1 := IndexCoordInfos{
		BaseComponentInfos: BaseComponentInfos{
			Name: ConstructComponentName(typeutil.IndexCoordRole, 1),
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestIndexCoordInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 IndexCoordInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1.Name, infos2.Name)
}

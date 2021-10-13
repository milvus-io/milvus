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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestBaseComponentInfos_Codec(t *testing.T) {
	infos1 := BaseComponentInfos{
		HasError:    false,
		ErrorReason: "",
		Name:        ConstructComponentName(typeutil.ProxyRole, 1),
		HardwareInfos: HardwareMetrics{
			IP:           "193.168.1.2",
			CPUCoreCount: 4,
			CPUCoreUsage: 0.5,
			Memory:       32 * 1024,
			MemoryUsage:  4 * 1024,
			Disk:         100 * 1024,
			DiskUsage:    2 * 1024,
		},
		SystemInfo: DeployMetrics{
			SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
			DeployMode:    ClusterDeployMode,
		},
		CreatedTime: time.Now().String(),
		UpdatedTime: time.Now().String(),
		Type:        typeutil.ProxyRole,
		ID:          1,
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestBaseComponentInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 BaseComponentInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

func TestQueryNodeInfos_Codec(t *testing.T) {
	infos1 := QueryNodeInfos{
		BaseComponentInfos: BaseComponentInfos{
			HasError:    false,
			ErrorReason: "",
			Name:        ConstructComponentName(typeutil.QueryNodeRole, 1),
			HardwareInfos: HardwareMetrics{
				IP:           "193.168.1.2",
				CPUCoreCount: 4,
				CPUCoreUsage: 0.5,
				Memory:       32 * 1024,
				MemoryUsage:  4 * 1024,
				Disk:         100 * 1024,
				DiskUsage:    2 * 1024,
			},
			SystemInfo: DeployMetrics{
				SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
				DeployMode:    ClusterDeployMode,
			},
			CreatedTime: time.Now().String(),
			UpdatedTime: time.Now().String(),
			Type:        typeutil.QueryNodeRole,
			ID:          1,
		},
		SystemConfigurations: QueryNodeConfiguration{
			SearchReceiveBufSize:         1024,
			SearchPulsarBufSize:          1024,
			SearchResultReceiveBufSize:   1024,
			RetrieveReceiveBufSize:       1024,
			RetrievePulsarBufSize:        1024,
			RetrieveResultReceiveBufSize: 1024,

			SimdType: "avx2",
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestQueryNodeInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 QueryNodeInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

func TestQueryCoordInfos_Codec(t *testing.T) {
	infos1 := QueryCoordInfos{
		BaseComponentInfos: BaseComponentInfos{
			HasError:    false,
			ErrorReason: "",
			Name:        ConstructComponentName(typeutil.QueryCoordRole, 1),
			HardwareInfos: HardwareMetrics{
				IP:           "193.168.1.2",
				CPUCoreCount: 4,
				CPUCoreUsage: 0.5,
				Memory:       32 * 1024,
				MemoryUsage:  4 * 1024,
				Disk:         100 * 1024,
				DiskUsage:    2 * 1024,
			},
			SystemInfo: DeployMetrics{
				SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
				DeployMode:    ClusterDeployMode,
			},
			CreatedTime: time.Now().String(),
			UpdatedTime: time.Now().String(),
			Type:        typeutil.QueryCoordRole,
			ID:          1,
		},
		SystemConfigurations: QueryCoordConfiguration{
			SearchChannelPrefix:       "search",
			SearchResultChannelPrefix: "search-result",
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestQueryCoordInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 QueryCoordInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

func TestIndexNodeInfos_Codec(t *testing.T) {
	infos1 := IndexNodeInfos{
		BaseComponentInfos: BaseComponentInfos{
			HasError:    false,
			ErrorReason: "",
			Name:        ConstructComponentName(typeutil.IndexNodeRole, 1),
			HardwareInfos: HardwareMetrics{
				IP:           "193.168.1.2",
				CPUCoreCount: 4,
				CPUCoreUsage: 0.5,
				Memory:       32 * 1024,
				MemoryUsage:  4 * 1024,
				Disk:         100 * 1024,
				DiskUsage:    2 * 1024,
			},
			SystemInfo: DeployMetrics{
				SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
				DeployMode:    ClusterDeployMode,
			},
			CreatedTime: time.Now().String(),
			UpdatedTime: time.Now().String(),
			Type:        typeutil.IndexNodeRole,
			ID:          1,
		},
		SystemConfigurations: IndexNodeConfiguration{
			MinioBucketName: "a-bucket",

			SimdType: "auto",
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestIndexNodeInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 IndexNodeInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

func TestIndexCoordInfos_Codec(t *testing.T) {
	infos1 := IndexCoordInfos{
		BaseComponentInfos: BaseComponentInfos{
			HasError:    false,
			ErrorReason: "",
			Name:        ConstructComponentName(typeutil.IndexCoordRole, 1),
			HardwareInfos: HardwareMetrics{
				IP:           "193.168.1.2",
				CPUCoreCount: 4,
				CPUCoreUsage: 0.5,
				Memory:       32 * 1024,
				MemoryUsage:  4 * 1024,
				Disk:         100 * 1024,
				DiskUsage:    2 * 1024,
			},
			SystemInfo: DeployMetrics{
				SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
				DeployMode:    ClusterDeployMode,
			},
			CreatedTime: time.Now().String(),
			UpdatedTime: time.Now().String(),
			Type:        typeutil.IndexCoordRole,
			ID:          1,
		},
		SystemConfigurations: IndexCoordConfiguration{
			MinioBucketName: "a-bucket",
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestIndexCoordInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 IndexCoordInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

func TestDataNodeInfos_Codec(t *testing.T) {
	infos1 := DataNodeInfos{
		BaseComponentInfos: BaseComponentInfos{
			HasError:    false,
			ErrorReason: "",
			Name:        ConstructComponentName(typeutil.DataNodeRole, 1),
			HardwareInfos: HardwareMetrics{
				IP:           "193.168.1.2",
				CPUCoreCount: 4,
				CPUCoreUsage: 0.5,
				Memory:       32 * 1024,
				MemoryUsage:  4 * 1024,
				Disk:         100 * 1024,
				DiskUsage:    2 * 1024,
			},
			SystemInfo: DeployMetrics{
				SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
				DeployMode:    ClusterDeployMode,
			},
			CreatedTime: time.Now().String(),
			UpdatedTime: time.Now().String(),
			Type:        typeutil.DataNodeRole,
			ID:          1,
		},
		SystemConfigurations: DataNodeConfiguration{
			FlushInsertBufferSize: 1024,
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestDataNodeInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 DataNodeInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

func TestDataCoordInfos_Codec(t *testing.T) {
	infos1 := DataCoordInfos{
		BaseComponentInfos: BaseComponentInfos{
			HasError:    false,
			ErrorReason: "",
			Name:        ConstructComponentName(typeutil.DataCoordRole, 1),
			HardwareInfos: HardwareMetrics{
				IP:           "193.168.1.2",
				CPUCoreCount: 4,
				CPUCoreUsage: 0.5,
				Memory:       32 * 1024,
				MemoryUsage:  4 * 1024,
				Disk:         100 * 1024,
				DiskUsage:    2 * 1024,
			},
			SystemInfo: DeployMetrics{
				SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
				DeployMode:    ClusterDeployMode,
			},
			CreatedTime: time.Now().String(),
			UpdatedTime: time.Now().String(),
			Type:        typeutil.DataCoordRole,
			ID:          1,
		},
		SystemConfigurations: DataCoordConfiguration{
			SegmentMaxSize: 1024 * 1024,
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestDataCoordInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 DataCoordInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

func TestRootCoordInfos_Codec(t *testing.T) {
	infos1 := RootCoordInfos{
		BaseComponentInfos: BaseComponentInfos{
			HasError:    false,
			ErrorReason: "",
			Name:        ConstructComponentName(typeutil.RootCoordRole, 1),
			HardwareInfos: HardwareMetrics{
				IP:           "193.168.1.2",
				CPUCoreCount: 4,
				CPUCoreUsage: 0.5,
				Memory:       32 * 1024,
				MemoryUsage:  4 * 1024,
				Disk:         100 * 1024,
				DiskUsage:    2 * 1024,
			},
			SystemInfo: DeployMetrics{
				SystemVersion: "8b1ae98fa97ce1c7ba853e8b9ff1c7ce24458dc1",
				DeployMode:    ClusterDeployMode,
			},
			CreatedTime: time.Now().String(),
			UpdatedTime: time.Now().String(),
			Type:        typeutil.RootCoordRole,
			ID:          1,
		},
		SystemConfigurations: RootCoordConfiguration{
			MinSegmentSizeToEnableIndex: 1024 * 10,
		},
	}
	s, err := MarshalComponentInfos(infos1)
	assert.Equal(t, nil, err)
	log.Info("TestRootCoordInfos_Codec",
		zap.String("marshaled_result", s))
	var infos2 RootCoordInfos
	err = UnmarshalComponentInfos(s, &infos2)
	assert.Equal(t, nil, err)
	assert.Equal(t, infos1, infos2)
}

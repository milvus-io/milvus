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

// This file is not used for now.
package configs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cases := []struct {
		name      string
		file      string
		content   string
		expectErr error
		expectCfg Config
	}{
		{
			"test load config",
			"milvus.test.config.1.toml",
			`timetick-interval = 200
            timetick-buffer-size = 512
            name-length-limit = 255
            field-count-limit = 256
            dimension-limit = 32768
            shard-count-limit = 256
            dml-channel-count = 256
            partition-count-limit = 4096
            enable-index-min-segment-size = 1024
            simd-type = "auto"
            skip-query-channel-recover = false
            [etcd]
            endpoints = ["localhost:2379"]
            use-embed = false`,
			nil,
			Config{
				TimeTickInterval:          200,
				TimeTickBufferSize:        512,
				NameLengthLimit:           255,
				FieldCountLimit:           256,
				DimensionLimit:            32768,
				ShardCountLimit:           256,
				DMLChannelCount:           256,
				PartitionCountLimit:       4096,
				EnableIndexMinSegmentSize: 1024,
				SIMDType:                  "auto",
				SkipQueryChannelRecover:   false,
				Etcd: Etcd{
					Endpoints: []string{"localhost:2379"},
					UseEmbed:  false,
				},
			},
		},
		{
			"test load undefied key",
			"milvus.test.config.2.toml",
			"undefied_key=200",
			&ErrUndecodedConfig{filepath.Join(tmpDir, "milvus.test.config.2.toml"), []string{"undefied_key"}},
			Config{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			filePath := filepath.Join(tmpDir, c.file)
			f, err := os.Create(filePath)
			assert.Nil(t, err)
			defer f.Close()
			_, err = f.WriteString(c.content)
			assert.Nil(t, err)
			cfg := Config{}
			err = cfg.Load(filePath)
			assert.EqualValues(t, c.expectErr, err)
			assert.EqualValues(t, c.expectCfg, cfg)
		})
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name      string
		cfg       *Config
		expectErr bool
	}{
		{
			"test validate success",
			&Config{
				Etcd: Etcd{
					UseEmbed: true,
				},
				Metrics: Metrics{
					DeployMode: metricsinfo.StandaloneDeployMode,
				},
				GarbageCollector: GarbageCollector{
					MissedFileTolerance:  MinToleranceTime,
					DroppedFileTolerance: MinToleranceTime,
				},
			},
			false,
		},
		{
			"test use embed etcd with cluster",
			&Config{
				Etcd: Etcd{
					UseEmbed: true,
				},
				Metrics: Metrics{
					DeployMode: metricsinfo.ClusterDeployMode,
				},
				GarbageCollector: GarbageCollector{
					MissedFileTolerance:  MinToleranceTime,
					DroppedFileTolerance: MinToleranceTime,
				},
			},
			true,
		},
		{
			"test use little tolerance time",
			&Config{
				Etcd: Etcd{
					UseEmbed: true,
				},
				Metrics: Metrics{
					DeployMode: metricsinfo.StandaloneDeployMode,
				},
				GarbageCollector: GarbageCollector{
					MissedFileTolerance:  MinToleranceTime - 1,
					DroppedFileTolerance: MinToleranceTime - 1,
				},
			},
			true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expectErr, c.cfg.Validate() != nil)
		})
	}
}

func TestEnforce(t *testing.T) {
	cases := []struct {
		name       string
		enforceEnv func(c *Config)
		enforceCmd func(c *Config)
		checkFunc  func(t *testing.T, c *Config)
	}{
		{"test no enforce func", nil, nil, func(t *testing.T, c *Config) {}},
		{
			"test enforce cmd func",
			func(c *Config) {
				c.TimeTickInterval = 1024
			},
			nil,
			func(t *testing.T, c *Config) {
				assert.EqualValues(t, 1024, c.TimeTickInterval)
			},
		},
		{
			"test enforceCmd override enforceEnv",
			func(c *Config) {
				c.TimeTickInterval = 512
			},
			func(c *Config) {
				c.TimeTickInterval = 1024
			},
			func(t *testing.T, c *Config) {
				assert.EqualValues(t, 1024, c.TimeTickInterval)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			InitializeConfig("", c.enforceEnv, c.enforceCmd)
			cfg := GetGlobalConfig()
			c.checkFunc(t, cfg)
		})
	}
}

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

package compaction

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// logKeyOfJSONTag turns a json tag such as "binlog_max_size" into the log key
// MarshalLogObject uses for it ("binlogMaxSize").
func logKeyOfJSONTag(tag string) string {
	parts := strings.Split(strings.Split(tag, ",")[0], "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

func TestParamsMarshalLogObject(t *testing.T) {
	params := Params{
		StorageVersion:            storage.StorageV2,
		StorageFormat:             "parquet",
		BinLogMaxSize:             1,
		UseMergeSort:              true,
		MaxSegmentMergeSort:       2,
		PreferSegmentSizeRatio:    0.5,
		BloomFilterApplyBatchSize: 3,
		UseLoonFFI:                true,
		LOBHoleRatioThreshold:     0.25,
		TextInlineThreshold:       4,
		TextMaxLobFileBytes:       5,
		TextFlushThresholdBytes:   6,
		StorageConfig: &indexpb.StorageConfig{
			StorageType:       "remote",
			Address:           "minio:9000",
			BucketName:        "a-bucket",
			RootPath:          "files",
			AccessKeyID:       "ak-secret",
			SecretAccessKey:   "sk-secret",
			SslCACert:         "ca-secret",
			GcpCredentialJSON: "gcp-secret",
		},
	}

	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info", DisableTimestamp: true})
	mlog.Info(context.Background(), "compact start", mlog.Any("compactionParams", params))
	logged := sink.String()

	// Every field of Params except StorageConfig must show up under the log key
	// derived from its json tag, so a field added later cannot silently vanish
	// from the logs.
	typ := reflect.TypeOf(params)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Name == "StorageConfig" {
			continue
		}
		tag := field.Tag.Get("json")
		require.NotEmptyf(t, tag, "field %s must carry a json tag", field.Name)
		assert.Contains(t, logged, logKeyOfJSONTag(tag)+"=", "field %s is missing from the log", field.Name)
	}

	for _, locator := range []string{"storageType=remote", "storageAddress=minio:9000", "storageBucket=a-bucket", "storageRootPath=files"} {
		assert.Contains(t, logged, locator)
	}
	for _, secret := range []string{"ak-secret", "sk-secret", "ca-secret", "gcp-secret"} {
		assert.NotContains(t, logged, secret)
	}
}

func TestGetJSONParams(t *testing.T) {
	paramtable.Init()
	jsonStr, err := GenerateJSONParams(nil)
	assert.NoError(t, err)

	storageVersion := storage.StorageV2
	if paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		storageVersion = storage.StorageV3
	}

	var result Params
	err = json.Unmarshal([]byte(jsonStr), &result)
	assert.NoError(t, err)
	assert.Equal(t, Params{
		StorageVersion:            storageVersion,
		StorageFormat:             paramtable.Get().DataNodeCfg.StorageFormat.GetValue(),
		BinLogMaxSize:             paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64(),
		UseMergeSort:              paramtable.Get().DataNodeCfg.UseMergeSort.GetAsBool(),
		MaxSegmentMergeSort:       paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.GetAsInt(),
		PreferSegmentSizeRatio:    paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat(),
		BloomFilterApplyBatchSize: paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt(),
		StorageConfig:             CreateStorageConfig(),
		UseLoonFFI:                paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool(),
		LOBHoleRatioThreshold:     GetLOBHoleRatioThreshold(),
		TextInlineThreshold:       getTextInlineThreshold(),
		TextMaxLobFileBytes:       getTextMaxLobFileBytes(),
		TextFlushThresholdBytes:   getTextFlushThresholdBytes(),
	}, result)
}

func TestCreateStorageConfigMaxConnections(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "minio")
	pt.Save(pt.MinioCfg.MaxConnections.Key, "237")
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.MinioCfg.MaxConnections.Key)
	})

	assert.Equal(t, uint32(237), CreateStorageConfig().GetMaxConnections())
}

// An external collection can reference an s3:// source while the cluster's
// primary storage is local, so the local branch must carry the connection cap
// too — otherwise the operator's minio.maxConnections is silently dropped for
// that topology.
func TestCreateStorageConfigMaxConnectionsLocalStorage(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.MinioCfg.MaxConnections.Key, "237")
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.MinioCfg.MaxConnections.Key)
	})

	assert.Equal(t, uint32(237), CreateStorageConfig().GetMaxConnections())
}

func TestGetParamsFromJSON(t *testing.T) {
	input := `{
		"storage_version": 0,
		"binlog_max_size": 4096,
		"use_merge_sort": false,
		"max_segment_merge_sort": 2,
		"prefer_segment_size_ratio": 0.1,
		"bloom_filter_apply_batch_size": 1000
	}`

	expected := Params{
		StorageVersion:            storage.StorageV1,
		BinLogMaxSize:             4096,
		UseMergeSort:              false,
		MaxSegmentMergeSort:       2,
		PreferSegmentSizeRatio:    0.1,
		BloomFilterApplyBatchSize: 1000,
	}

	result, err := ParseParamsFromJSON(input)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetParamsFromJSON_InvalidJSON(t *testing.T) {
	invalidJSON := `{ this is not valid json }`
	_, err := ParseParamsFromJSON(invalidJSON)
	assert.Error(t, err)
}

func TestGetParamsFromJSON_EmptyJSON(t *testing.T) {
	// Test compatibility
	emptyJSON := ``
	_, err := ParseParamsFromJSON(emptyJSON)
	assert.Error(t, err)
}

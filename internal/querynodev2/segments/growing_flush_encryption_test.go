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

package segments

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func encryptedGrowingFlushConfig() *FlushConfig {
	return &FlushConfig{
		CollectionID: 42,
		Schema: &schemapb.CollectionSchema{
			Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "17"}},
		},
		WriterFormat:       "parquet",
		SchemaBasedPattern: "0,1,100;101",
		SchemaBasedFormats: "parquet,parquet",
	}
}

func TestGrowingFlushEncryptionZone(t *testing.T) {
	config := encryptedGrowingFlushConfig()
	ezID, err := growingFlushEncryptionZone(config)
	require.NoError(t, err)
	require.Equal(t, int64(17), ezID)
}

func TestGrowingFlushPlaintextDoesNotRequireCipher(t *testing.T) {
	config := encryptedGrowingFlushConfig()
	config.Schema.Properties = nil
	config.TextFieldIDs = []int64{102}
	config.WriterFormat = "vortex"
	ezID, err := growingFlushEncryptionZone(config)
	require.NoError(t, err)
	require.Zero(t, ezID)
}

func TestGrowingFlushPropagatesWriterEncryptionFailure(t *testing.T) {
	patch := mockey.Mock(packed.WriterEncryptionProperties).To(func(ezID, collectionID int64) (map[string]string, error) {
		require.Equal(t, int64(17), ezID)
		require.Equal(t, int64(42), collectionID)
		return nil, merr.ErrServiceInternal
	}).Build()
	defer patch.UnPatch()
	// A missing native segment makes reaching the write path a test failure.
	segment := &LocalSegment{baseSegment: baseSegment{segmentType: SegmentTypeGrowing}}
	result, err := segment.FlushData(context.Background(), 0, 1, encryptedGrowingFlushConfig())
	require.Nil(t, result)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, 1, patch.Times())
}

func TestGrowingFlushEncryptionFailsBeforeNativeWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*FlushConfig)
		want   string
		code   error
	}{
		{name: "invalid EZ", change: func(c *FlushConfig) { c.Schema.Properties[0].Value = "bad-ez-canary" }, want: "invalid encryption zone ID", code: merr.ErrServiceInternal},
		{name: "empty EZ", change: func(c *FlushConfig) { c.Schema.Properties[0].Value = "" }, want: "invalid encryption zone ID", code: merr.ErrServiceInternal},
		{name: "zero EZ", change: func(c *FlushConfig) { c.Schema.Properties[0].Value = "0" }, want: "invalid encryption zone ID", code: merr.ErrServiceInternal},
		{name: "invalid collection", change: func(c *FlushConfig) { c.CollectionID = 0 }, want: "invalid collection ID", code: merr.ErrServiceInternal},
		{name: "TEXT LOB", change: func(c *FlushConfig) { c.TextFieldIDs = []int64{102} }, want: "does not support TEXT/LOB", code: merr.ErrServiceUnimplemented},
		{name: "Vortex", change: func(c *FlushConfig) { c.WriterFormat = "vortex" }, want: "requires Parquet", code: merr.ErrServiceUnimplemented},
		{name: "mixed groups", change: func(c *FlushConfig) { c.SchemaBasedFormats = "parquet,vortex" }, want: "requires Parquet column groups", code: merr.ErrServiceUnimplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := encryptedGrowingFlushConfig()
			if tc.change != nil {
				tc.change(config)
			}
			// No native segment exists: the specific encryption error must be
			// returned before any data extraction or writer can run.
			segment := &LocalSegment{baseSegment: baseSegment{segmentType: SegmentTypeGrowing}}
			result, err := segment.FlushData(context.Background(), 0, 1, config)
			require.Nil(t, result)
			require.ErrorIs(t, err, tc.code)
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), "bad-ez-canary")
		})
	}
}

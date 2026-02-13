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

package replicateutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestConfigLogField(t *testing.T) {
	enc := zapcore.NewJSONEncoder(zapcore.EncoderConfig{})

	t.Run("with_clusters_and_topology", func(t *testing.T) {
		config := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{
					ClusterId:       "cluster-a",
					ConnectionParam: &commonpb.ConnectionParam{Uri: "http://cluster-a:19530"},
					Pchannels:       []string{"channel-1", "channel-2"},
				},
				{
					ClusterId:       "cluster-b",
					ConnectionParam: &commonpb.ConnectionParam{Uri: "http://cluster-b:19530"},
					Pchannels:       []string{"channel-3"},
				},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "cluster-a", TargetClusterId: "cluster-b"},
			},
		}

		field := ConfigLogField(config)
		buf, err := enc.EncodeEntry(zapcore.Entry{}, []zapcore.Field{field})
		assert.NoError(t, err)
		output := buf.String()
		assert.Contains(t, output, "cluster-a")
		assert.Contains(t, output, "cluster-b")
		assert.Contains(t, output, "cluster-a->cluster-b")
		assert.Contains(t, output, "http://cluster-a:19530")
	})

	t.Run("nil_config", func(t *testing.T) {
		field := ConfigLogField(nil)
		buf, err := enc.EncodeEntry(zapcore.Entry{}, []zapcore.Field{field})
		assert.NoError(t, err)
		assert.NotEmpty(t, buf.String())
	})

	t.Run("empty_config", func(t *testing.T) {
		config := &commonpb.ReplicateConfiguration{}
		field := ConfigLogField(config)
		buf, err := enc.EncodeEntry(zapcore.Entry{}, []zapcore.Field{field})
		assert.NoError(t, err)
		assert.NotEmpty(t, buf.String())
	})
}

// Ensure buffer.Buffer satisfies the io.Writer for the encoder
var _ interface{ String() string } = (*buffer.Buffer)(nil)

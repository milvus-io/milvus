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

package indexpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestStorageConfigTalonStageMaskSurvivesWireConversions(t *testing.T) {
	configs := []proto.Message{&indexpb.StorageConfig{}, &indexcgopb.StorageConfig{}, &clusteringpb.StorageConfig{}}
	for _, mask := range []uint32{0, 1, 16, ^uint32(0)} {
		// These messages are converted by marshaling one schema and unmarshaling
		// another. Use the wire contract directly to catch a missing/misnumbered
		// field, including a future stage bit that converters must preserve.
		wire := protowire.AppendTag(nil, 19, protowire.VarintType)
		wire = protowire.AppendVarint(wire, 2)
		if mask != 0 {
			wire = protowire.AppendTag(wire, 25, protowire.VarintType)
			wire = protowire.AppendVarint(wire, uint64(mask))
		}
		for _, source := range configs {
			require.NoError(t, proto.Unmarshal(wire, source))
			encoded, err := proto.Marshal(source)
			require.NoError(t, err)
			for _, targetType := range configs {
				target := targetType.ProtoReflect().New()
				require.NoError(t, proto.Unmarshal(encoded, target.Interface()))
				fields := target.Descriptor().Fields()
				modeField := fields.ByName("talon_mode")
				stageField := fields.ByName("talon_enabled_stages")
				require.NotNil(t, stageField, "missing mask in %s", target.Descriptor().FullName())
				require.Equal(t, protoreflect.Uint32Kind, stageField.Kind())
				require.Equal(t, uint64(2), target.Get(modeField).Uint())
				require.Equal(t, uint64(mask), target.Get(stageField).Uint())
			}
		}
	}
}

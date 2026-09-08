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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestIsMatchEnabledEquivalent(t *testing.T) {
	for _, kind := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text, schemapb.DataType_JSON, schemapb.DataType_Int64} {
		for _, value := range []string{"true", "false", "TRUE", "1", "0", "bad", ""} {
			for _, duplicate := range []bool{false, true} {
				field := &schemapb.FieldSchema{DataType: kind, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: value}}}
				if duplicate {
					field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{Key: "enable_match", Value: "false"})
				}
				require.Equal(t, CreateFieldSchemaHelper(field).EnableMatch(), IsMatchEnabled(field))
			}
		}
		field := &schemapb.FieldSchema{DataType: kind}
		require.Equal(t, CreateFieldSchemaHelper(field).EnableMatch(), IsMatchEnabled(field))
	}
	require.False(t, IsMatchEnabled(nil))
}

func TestIsMatchEnabledNoAllocation(t *testing.T) {
	for _, params := range [][]*commonpb.KeyValuePair{nil, {{Key: "enable_match", Value: "true"}}} {
		field := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar, TypeParams: params}
		require.Zero(t, testing.AllocsPerRun(100, func() { IsMatchEnabled(field) }))
	}
}

func BenchmarkMatchFieldCheck(b *testing.B) {
	field := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}
	b.Run("legacy_helper", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			CreateFieldSchemaHelper(field).EnableMatch()
		}
	})
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			IsMatchEnabled(field)
		}
	})
}

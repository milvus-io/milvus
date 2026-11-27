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

package schemautil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStructSubFieldOriginMarker(t *testing.T) {
	field := &schemapb.FieldSchema{
		Name:       "s[b]",
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "8"}},
	}
	require.False(t, IsAddedStructSubField(field))
	require.NoError(t, RejectStructSubFieldOriginMarker(field))

	MarkAddedStructSubField(field)
	MarkAddedStructSubField(field)
	require.True(t, IsAddedStructSubField(field))
	require.Len(t, field.GetTypeParams(), 2)
	require.ErrorIs(t, RejectStructSubFieldOriginMarker(field), merr.ErrParameterInvalid)

	original := field.GetTypeParams()
	stripped := StripStructSubFieldOriginMarker(original)
	require.Equal(t, []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "8"}}, stripped)
	require.Len(t, original, 2, "strip must not modify its input")
	plain := []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "8"}}
	require.Equal(t, plain, StripStructSubFieldOriginMarker(plain))

	require.NoError(t, RejectStructSubFieldOriginMarkerKeys("f", common.MaxCapacityKey))
	require.ErrorIs(t, RejectStructSubFieldOriginMarkerKeys("f", common.MaxCapacityKey, common.StructSubFieldAddedKey), merr.ErrParameterInvalid)
}

func TestCheckStructKeepsOriginalSubField(t *testing.T) {
	original := &schemapb.FieldSchema{Name: "s[a]"}
	added := &schemapb.FieldSchema{Name: "s[b]"}
	MarkAddedStructSubField(added)

	require.NoError(t, CheckStructKeepsOriginalSubField("s", "s[c]", []*schemapb.FieldSchema{original, added}))
	err := CheckStructKeepsOriginalSubField("s", "s[a]", []*schemapb.FieldSchema{added})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, `cannot drop sub-field "s[a]" of struct field "s"`)
}

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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestGetAddedLoadFieldIDs(t *testing.T) {
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}}}
	next := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 0},
		{FieldID: 100},
		{FieldID: 101},
		{FieldID: 102},
		{FieldID: 103, TypeParams: []*commonpb.KeyValuePair{{Key: FieldSkipLoadKey, Value: "true"}}},
		{FieldID: 104, IsDynamic: true},
		{FieldID: 105, TypeParams: []*commonpb.KeyValuePair{{Key: FieldSkipLoadKey, Value: "invalid"}}},
	}, StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 106, Fields: []*schemapb.FieldSchema{
		{FieldID: 107}, {FieldID: 108, TypeParams: []*commonpb.KeyValuePair{{Key: FieldSkipLoadKey, Value: "true"}}},
	}}}}
	require.Equal(t, []int64{102, 104, 105, 107}, GetAddedLoadFieldIDs(old, next))
	require.Empty(t, GetAddedLoadFieldIDs(next, next), "properties-only updates do not repair old omissions")
	require.Empty(t, GetAddedLoadFieldIDs(next, old), "dropping fields does not add preload hints")
	require.Empty(t, GetAddedLoadFieldIDs(nil, nil))
}

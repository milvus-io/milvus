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

package rootcoord

import (
	"context"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// InvalidCollectionID mirrors the constant historically defined in
// internal/rootcoord; used by the moved MetaTable implementation.
const InvalidCollectionID = UniqueID(0)

func isMaxTs(ts Timestamp) bool {
	return ts == typeutil.MaxTimestamp
}

func MaxAssignedFieldIDFromSchema(schema *schemapb.CollectionSchema) int64 {
	maxFieldID := int64(common.StartOfUserFieldID)
	if schema == nil {
		return maxFieldID
	}
	for _, field := range schema.GetFields() {
		if field.GetFieldID() > maxFieldID {
			maxFieldID = field.GetFieldID()
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		if structField.GetFieldID() > maxFieldID {
			maxFieldID = structField.GetFieldID()
		}
		for _, subField := range structField.GetFields() {
			if subField.GetFieldID() > maxFieldID {
				maxFieldID = subField.GetFieldID()
			}
		}
	}
	for _, kv := range schema.GetProperties() {
		if kv.GetKey() != common.MaxFieldIDKey {
			continue
		}
		v, err := strconv.ParseInt(kv.GetValue(), 10, 64)
		if err != nil {
			mlog.Warn(context.TODO(), "failed to parse max_field_id property, metadata may be corrupted",
				mlog.String("value", kv.GetValue()),
				mlog.Err(err),
			)
		} else if v > maxFieldID {
			maxFieldID = v
		}
		break
	}
	return maxFieldID
}

// UpdateMaxFieldIDProperty returns a new properties slice with max_field_id set.
// The original slice is not modified.
func UpdateMaxFieldIDProperty(properties []*commonpb.KeyValuePair, maxFieldID int64) []*commonpb.KeyValuePair {
	result := make([]*commonpb.KeyValuePair, 0, len(properties)+1)
	found := false
	for _, kv := range properties {
		if kv.GetKey() == common.MaxFieldIDKey {
			v, err := strconv.ParseInt(kv.GetValue(), 10, 64)
			if err != nil {
				mlog.Warn(context.TODO(), "failed to parse max_field_id property, metadata may be corrupted",
					mlog.String("value", kv.GetValue()),
					mlog.Err(err),
				)
			} else if v > maxFieldID {
				maxFieldID = v
			}
			result = append(result, &commonpb.KeyValuePair{
				Key:   common.MaxFieldIDKey,
				Value: strconv.FormatInt(maxFieldID, 10),
			})
			found = true
			continue
		}
		result = append(result, kv)
	}
	if !found {
		result = append(result, &commonpb.KeyValuePair{
			Key:   common.MaxFieldIDKey,
			Value: strconv.FormatInt(maxFieldID, 10),
		})
	}
	return result
}

func ensureCollectionMaxFieldIDProperty(coll *model.Collection) {
	if coll == nil {
		return
	}
	coll.Properties = UpdateMaxFieldIDProperty(coll.Properties, MaxAssignedFieldIDFromSchema(coll.ToCollectionSchemaPB()))
}

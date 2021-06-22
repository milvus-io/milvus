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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDummyRequestType(t *testing.T) {
	invalidStr := `{"request_type":"retrieve"`
	_, err := parseDummyRequestType(invalidStr)
	assert.NotNil(t, err)

	retrievetypeStr := `{"request_type":"retrieve"}`
	drt, err := parseDummyRequestType(retrievetypeStr)
	assert.Nil(t, err)
	assert.Equal(t, drt.RequestType, "retrieve")
}

func TestParseDummyRetrieveRequest(t *testing.T) {
	invalidStr := `{"request_type":"retrieve"`
	_, err := parseDummyRetrieveRequest(invalidStr)
	assert.NotNil(t, err)

	onlytypeStr := `{"request_type":"retrieve"}`
	drr, err := parseDummyRetrieveRequest(onlytypeStr)
	assert.Nil(t, err)
	assert.Equal(t, drr.RequestType, "retrieve")
	assert.Equal(t, len(drr.DbName), 0)

	fulltypeStr := `{
	"request_type":"retrieve",
	"dbname":"",
	"collection_name":"test",
	"partition_names": [],
	"ids": [100, 101],
	"output_fields": ["_id", "age"]
	}`
	drr2, err := parseDummyRetrieveRequest(fulltypeStr)
	assert.Nil(t, err)
	assert.Equal(t, drr2.RequestType, "retrieve")
	assert.Equal(t, len(drr2.DbName), 0)
	assert.Equal(t, drr2.CollectionName, "test")
	assert.Equal(t, len(drr2.PartitionNames), 0)
	assert.Equal(t, len(drr2.Ids), 2)
	assert.Equal(t, drr2.Ids, []int64{100, 101})
	assert.Equal(t, drr2.OutputFields, []string{"_id", "age"})
}

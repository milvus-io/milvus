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

package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGetExpectedSegmentSizeCappedByCeiling(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, typeutil.SizeMetricMainIndex)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key, "64")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key)
	})

	indexMeta := &index.IndexMeta{}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 10, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
	}}
	// SegmentMaxSize default 1024MB is capped to the 64MB ceiling.
	assert.Equal(t, int64(64*1024*1024), GetExpectedSegmentSize(indexMeta, 1, schema))
}

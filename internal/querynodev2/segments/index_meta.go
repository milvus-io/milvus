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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ComposeIndexMeta builds segcore CollectionIndexMeta from coordinator index infos.
func ComposeIndexMeta(ctx context.Context, indexInfos []*indexpb.IndexInfo, schema *schemapb.CollectionSchema) *segcorepb.CollectionIndexMeta {
	fieldIndexMetas := make([]*segcorepb.FieldIndexMeta, 0, len(indexInfos))
	for _, info := range indexInfos {
		fieldIndexMetas = append(fieldIndexMetas, &segcorepb.FieldIndexMeta{
			CollectionID:    info.GetCollectionID(),
			FieldID:         info.GetFieldID(),
			IndexName:       info.GetIndexName(),
			TypeParams:      info.GetTypeParams(),
			IndexParams:     info.GetIndexParams(),
			IsAutoIndex:     info.GetIsAutoIndex(),
			UserIndexParams: info.GetUserIndexParams(),
		})
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	maxIndexRecordPerSegment := int64(0)
	if err != nil || sizePerRecord == 0 {
		mlog.Warn(ctx, "failed to transfer segment size to collection, because failed to estimate size per record", mlog.Err(err))
	} else {
		threshold := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
		proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
		maxIndexRecordPerSegment = int64(threshold * proportion / float64(sizePerRecord))
	}

	return &segcorepb.CollectionIndexMeta{
		IndexMetas:       fieldIndexMetas,
		MaxIndexRowCount: maxIndexRecordPerSegment,
	}
}

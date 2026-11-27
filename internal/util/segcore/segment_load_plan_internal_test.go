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

package segcore

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

func TestExternalRowEstimateBeforeNativeConversion(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, ExternalField: "value", DataType: schemapb.DataType_Int64}}}
	collection := &CCollection{schema: schema}
	for _, operation := range []string{"create", "prepared_reopen", "legacy_reopen"} {
		t.Run(operation, func(t *testing.T) {
			info := &querypb.SegmentLoadInfo{
				NumOfRows:   10,
				BinlogPaths: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 1000}}}},
			}
			stop := errors.New("stop before native call")
			called := false
			patch := mockey.Mock(convertToSegcoreSegmentLoadInfo).To(func(input *querypb.SegmentLoadInfo, origin packed.ManifestReadOrigin) (*segcorepb.SegmentLoadInfo, error) {
				called = true
				require.EqualValues(t, 100, input.GetEstimatedBytesPerRow())
				if operation == "create" {
					require.Equal(t, packed.ManifestReadCreate, origin)
				} else {
					require.Equal(t, packed.ManifestReadReopen, origin)
				}
				return nil, stop
			}).Build()
			defer patch.UnPatch()
			var err error
			switch operation {
			case "create":
				_, err = CreateCSegment(&CreateCSegmentRequest{Collection: collection, LoadInfo: info})
			case "prepared_reopen":
				_, err = PrepareSegmentLoadPlan(context.Background(), nil, schema, 0, info, false)
			case "legacy_reopen":
				err = (&cSegmentImpl{}).Reopen(context.Background(), &ReopenRequest{Collection: collection, LoadInfo: info})
			}
			require.True(t, called)
			require.ErrorIs(t, err, stop)
		})
	}
}

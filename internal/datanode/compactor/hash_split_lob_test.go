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

package compactor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// Hash routing consumes logical TEXT values and writes new LOBs for each
// target. Read the outputs after removing the source LOBs to catch dangling
// references, including legacy sources whose partition namespace differs.
func TestHashSplitTextLOBRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name               string
		legacy, inlineOnly bool
	}{
		{name: "canonical"}, {name: "legacy", legacy: true}, {name: "inline source lower output threshold", inlineOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupBumpUTEnv(t)
			paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "64")
			defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key)
			const textID = int64(105)
			const rows = 12
			now := time.Now()
			rowTime := func(i int) uint64 {
				if i == rows-1 {
					return tsoutil.ComposeTSByTime(now.Add(-2 * time.Hour))
				}
				return tsoutil.ComposeTSByTime(now)
			}
			bodies := make(map[int64]any, rows)
			opts := []fixtureOpt{
				withRows(rows), withoutSourceText(),
				withSourceFields(&schemapb.FieldSchema{FieldID: textID, Name: "body", DataType: schemapb.DataType_Text, Nullable: true}),
				withTextLOBSource(textID), withCollectionTTL(time.Hour), withRowTs(rowTime),
				withDeletedPKs(map[any]uint64{int64(3): rowTime(3) + 1}),
				withFillValue(func(i int, _ uint64, v map[int64]any) {
					var body any
					switch i % 4 {
					case 0:
						body = ""
					case 1:
						body = "猫"
					case 2:
						body = fmt.Sprintf("row-%d-", i) + strings.Repeat("x", 90000)
					}
					bodies[int64(i)] = body
					v[textID] = body
				}),
			}
			if tc.legacy {
				opts = append(opts, withLegacySourceNamespace())
			}
			if tc.inlineOnly {
				paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "1000000")
			}
			fix := buildBumpFixture(t, opts...)
			sourceLOBs, err := packed.GetManifestLobFiles(fix.sourceManifest, fix.cfg)
			require.NoError(t, err)
			if tc.inlineOnly {
				require.Empty(t, sourceLOBs)
			} else {
				require.NotEmpty(t, sourceLOBs, "fixture must contain external LOB bodies")
			}
			paramtable.Get().Save(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "64")
			plan := fix.task.plan
			plan.Type = datapb.CompactionType_SplitShardRewriteCompaction
			plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 19531, End: 19631}
			plan.SplitRewrite = &datapb.SplitShardRewritePlan{RoutingModulus: 2, Targets: doublingTargets(0, 1), TtlCutoffTimetick: tsoutil.ComposeTSByTime(now)}
			// Follow the dispatch path: GenerateJSONParams keeps TEXT on V3 even
			// while the generic UseLoonFFI storage configuration is disabled.
			plan.JsonParams, err = compaction.GenerateJSONParams(plan.GetSchema())
			require.NoError(t, err)
			params, err := compaction.ParseParamsFromJSON(plan.GetJsonParams())
			require.NoError(t, err)
			require.EqualValues(t, storage.StorageV3, params.StorageVersion)
			task := NewSplitShardRewriteCompactionTask(context.Background(), binlogio.NewBinlogIO(fix.task.chunkManager), plan, params)
			result, err := task.Compact()
			require.NoError(t, err)
			require.Len(t, result.GetSegments(), 2)
			for _, lob := range sourceLOBs {
				require.NoError(t, os.Rename(lob.Path, lob.Path+".unavailable"))
			}
			expected := make(map[int64]any)
			for pk, body := range bodies {
				if pk != 3 && pk != rows-1 {
					expected[pk] = body
				}
			}
			partitionBase := storage.SegmentPartitionBasePath(fix.cfg.GetRootPath(), CollectionID, PartitionID)
			configs := []packed.TextColumnConfig{{FieldID: textID, LobBasePath: path.Join(partitionBase, "lobs", fmt.Sprint(textID))}}
			for _, out := range result.GetSegments() {
				outputBase, _, err := packed.UnmarshalManifestPath(out.GetManifest())
				require.NoError(t, err)
				require.Equal(t, storage.SegmentManifestBasePath(fix.cfg.GetRootPath(), CollectionID, PartitionID, out.GetSegmentID()), outputBase)
				outputLOBs, err := packed.GetManifestLobFiles(out.GetManifest(), fix.cfg)
				require.NoError(t, err)
				require.NotEmpty(t, outputLOBs, "lower output threshold must produce fresh LOBs even from an inline-only source")
				for _, lob := range outputLOBs {
					require.True(t, strings.HasPrefix(lob.Path, configs[0].LobBasePath+"/"))
				}
				reader, err := storage.NewTextDecodedManifestRecordReader(context.Background(), out.GetManifest(), fix.targetSchema, configs,
					storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(fix.cfg))
				require.NoError(t, err)
				var count, payloadBytes int
				for {
					rec, err := reader.Next()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					pks := rec.Column(bumpFxPKField).(*array.Int64)
					values := rec.Column(textID).(*array.String)
					for i := 0; i < rec.Len(); i++ {
						pk := pks.Value(i)
						body, ok := expected[pk]
						require.True(t, ok, "unexpected or repeated pk %d", pk)
						target, err := routing.PKResidueInt64(pk, 2)
						require.NoError(t, err)
						require.Equal(t, plan.GetSplitRewrite().GetTargets()[target].GetVchannel(), out.GetSplitTarget())
						if body == nil {
							require.True(t, values.IsNull(i))
						} else {
							require.False(t, values.IsNull(i))
							require.Equal(t, body, values.Value(i))
							payloadBytes += len(body.(string))
						}
						delete(expected, pk)
						count++
					}
				}
				require.NoError(t, reader.Close())
				require.EqualValues(t, count, out.GetNumOfRows())
				sizes := out.GetStats().GetFieldLogicalSizes()
				require.EqualValues(t, count, sizes.GetRowCount())
				require.Equal(t, &datapb.FieldLogicalSize{PayloadBytes: uint64(payloadBytes), ValidityBits: uint64(count)}, sizes.GetFields()[textID])
			}
			require.Empty(t, expected, "every live row must survive exactly once")
		})
	}
}

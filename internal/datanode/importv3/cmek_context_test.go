// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importv3

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

type reshardSourceCipher struct {
	hook.Cipher
	key     []byte
	lookups [][2]int64
}

func (c *reshardSourceCipher) GetUnsafeKey(ezID, collectionID int64) []byte {
	c.lookups = append(c.lookups, [2]int64{ezID, collectionID})
	return c.key
}

func TestReshardSeparatesSourceAndTargetEncryption(t *testing.T) {
	initReshardPipelineParams(t)
	for _, sourceEncrypted := range []bool{false, true} {
		for _, targetEncrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("source=%t/target=%t", sourceEncrypted, targetEncrypted), func(t *testing.T) {
				fix := newReshardPipelineFixture()
				plan := fix.plan([]*internalpb.ImportFile{fix.source(1)}, 8<<20)
				plan.CollectionId = 23
				plan.Options = []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}}
				cipher := &reshardSourceCipher{key: []byte("source-key")}
				plugin := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
				t.Cleanup(func() { plugin.UnPatch() })
				var source *indexcgopb.StoragePluginContext
				var target *indexcgopb.StoragePluginContext
				if targetEncrypted {
					target = &indexcgopb.StoragePluginContext{EncryptionZoneId: 37, CollectionId: 23, EncryptionKey: base64.StdEncoding.EncodeToString([]byte("target-key"))}
					plan.CollectionSchema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "37"}}
				}
				if sourceEncrypted {
					plan.Options = append(plan.Options, &commonpb.KeyValuePair{Key: importutilv2.EZK, Value: base64.StdEncoding.EncodeToString([]byte(`{"ez_id":31}`))})
					source = &indexcgopb.StoragePluginContext{EncryptionZoneId: 31, CollectionId: 0, EncryptionKey: base64.StdEncoding.EncodeToString(cipher.key)}
				}

				batch := fix.batch(1, 2)
				batch.Data[common.RowIDField] = &storage.Int64FieldData{Data: []int64{101, 102}}
				batch.Data[common.TimeStampField] = &storage.Int64FieldData{Data: []int64{103, 104}}
				recorder := &reshardCallRecorder{}
				mockReshardBoundaries(t, map[int64]reshardReaderBuilder{
					1: staticReshardReader(&scriptReader{batches: []*storage.InsertData{batch}}),
				}, recorder)
				require.NoError(t, executeReshardPlan(context.Background(), nil, fix.request(plan, 1), plan, target, NewMetrics(1), nil))
				require.Equal(t, []*indexcgopb.StoragePluginContext{source}, recorder.readContexts)
				require.Equal(t, []*indexcgopb.StoragePluginContext{target}, recorder.writeContexts)
				require.Equal(t, []int64{2}, recorder.rows)
				require.EqualValues(t, 1, recorder.publishCalled.Load())
				if sourceEncrypted {
					require.Equal(t, [][2]int64{{31, 0}}, cipher.lookups)
				} else {
					require.Empty(t, cipher.lookups)
				}
			})
		}
	}
}

func TestReshardRejectsInvalidSourceEncryptionBeforeIO(t *testing.T) {
	initReshardPipelineParams(t)
	for _, tc := range []struct {
		name       string
		ezk        string
		wantError  string
		wantLookup bool
	}{
		{"invalid base64", "!", "failed to decode EZK", false},
		{"invalid JSON", base64.StdEncoding.EncodeToString([]byte("not JSON")), "failed to unmarshal EZK", false},
		{"unavailable source key", base64.StdEncoding.EncodeToString([]byte(`{"ez_id":31}`)), "cannot get ez key", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fix := newReshardPipelineFixture()
			plan := fix.plan([]*internalpb.ImportFile{fix.source(1)}, 8<<20)
			plan.CollectionId = 23
			plan.Options = []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}, {Key: importutilv2.EZK, Value: tc.ezk}}
			cipher := &reshardSourceCipher{}
			plugin := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
			t.Cleanup(func() { plugin.UnPatch() })
			recorder := &reshardCallRecorder{}
			mockReshardBoundaries(t, nil, recorder)
			// A usable target context must not hide a source-context failure.
			target := &indexcgopb.StoragePluginContext{EncryptionZoneId: 37, CollectionId: 23, EncryptionKey: base64.StdEncoding.EncodeToString([]byte("target-key"))}
			err := executeReshardPlan(context.Background(), nil, fix.request(plan, 1), plan, target, NewMetrics(1), nil)
			require.ErrorContains(t, err, tc.wantError)
			require.Empty(t, recorder.readContexts)
			require.Empty(t, recorder.writeContexts)
			require.Zero(t, recorder.publishCalled.Load())
			if tc.wantLookup {
				require.Equal(t, [][2]int64{{31, 0}}, cipher.lookups)
			} else {
				require.Empty(t, cipher.lookups)
			}
		})
	}
}

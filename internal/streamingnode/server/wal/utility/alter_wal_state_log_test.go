// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utility

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestAlterWALStateLogFieldsOmitConfigPayload(t *testing.T) {
	assert.Empty(t, AlterWALStateLogFields(nil))

	state := &streamingpb.AlterWALState{
		Stage: streamingpb.AlterWALStage_FLUSHING,
		Configs: map[string]string{
			"ssl.key.pem":   "inline-private-key",
			"sasl.password": "broker-secret",
		},
	}
	fields := AlterWALStateLogFields(state)
	require.Len(t, fields, 3)
	assert.Equal(t, "alterWALStage", fields[0].Key)
	assert.Equal(t, "FLUSHING", fields[0].String)
	assert.Equal(t, "targetWALName", fields[1].Key)
	assert.Equal(t, "alterWALConfigCount", fields[2].Key)
	assert.Equal(t, int64(2), fields[2].Integer)
	for _, payload := range []string{"ssl.key.pem", "sasl.password", "inline-private-key", "broker-secret"} {
		for _, field := range fields {
			assert.NotContains(t, field.Key, payload)
			assert.NotContains(t, field.String, payload)
		}
	}
}

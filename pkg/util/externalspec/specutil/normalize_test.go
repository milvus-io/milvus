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

package specutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeExternalSpec(t *testing.T) {
	for _, tc := range []struct {
		name, input, want string
	}{
		{"format", `{"Format":"parquet"}`, `{"format":"parquet"}`},
		{"columns", `{"Columns":["Vector"]}`, `{"columns":["Vector"]}`},
		{"extfs", `{"EXTFS":{"region":"US-East-1"}}`, `{"extfs":{"region":"US-East-1"}}`},
		{"snapshot precision", `{"Snapshot_Id":5320540205222981137}`, `{"snapshot_id":"5320540205222981137"}`},
		{"unicode fold", `{"Format":"paimon-table","ſnapshot_id":"7","ſcan_mode":"data-split"}`, `{"format":"paimon-table","snapshot_id":"7","scan_mode":"data-split"}`},
		{"last variant", `{"format":"parquet","FORMAT":"vortex"}`, `{"format":"vortex"}`},
		{"last canonical", `{"FORMAT":"vortex","format":"parquet"}`, `{"format":"parquet"}`},
		{"identical keys", `{"format":"vortex","format":"parquet"}`, `{"format":"parquet"}`},
		{"null string keeps value", `{"format":"vortex","FORMAT":null}`, `{"format":"vortex"}`},
		{"null snapshot clears value", `{"snapshot_id":7,"SNAPSHOT_ID":null}`, `{"snapshot_id":null}`},
		{"duplicate maps merge", `{"extfs":{"region":"us-east-1"},"EXTFS":{"use_ssl":"true"}}`, `{"extfs":{"region":"us-east-1","use_ssl":"true"}}`},
		{"null map clears value", `{"extfs":{"region":"us-east-1"},"EXTFS":null}`, `{"extfs":null}`},
		{"duplicate arrays replace", `{"columns":["a"],"COLUMNS":["b"]}`, `{"columns":["b"]}`},
		{"null array clears value", `{"columns":["a"],"COLUMNS":null}`, `{"columns":null}`},
		{"scan mode missing", `{"Format":"paimon-table"}`, `{"format":"paimon-table"}`},
		{"scan mode null", `{"Format":"paimon-table","Scan_Mode":null}`, `{"format":"paimon-table","scan_mode":""}`},
		{"scan mode duplicate", `{"Format":"paimon-table","SCAN_MODE":"data-split","scan_mode":"direct-file"}`, `{"format":"paimon-table","scan_mode":"direct-file"}`},
		{"unknown values", `{"Format":"parquet","CUSTOM":{"Format":"untouched","n":9007199254740993,"f":1.2300},"custom":null}`, `{"format":"parquet","CUSTOM":{"Format":"untouched","n":9007199254740993,"f":1.2300},"custom":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := ParseExternalSpec(tc.input)
			require.NoError(t, err)
			got, err := NormalizeExternalSpec(tc.input)
			require.NoError(t, err)
			var want, actual map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tc.want), &want))
			require.NoError(t, json.Unmarshal([]byte(got), &actual))
			require.Equal(t, want, actual)
			again, err := NormalizeExternalSpec(got)
			require.NoError(t, err)
			require.Equal(t, got, again)
			after, err := ParseExternalSpec(got)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestNormalizeExternalSpecInvalidTypes(t *testing.T) {
	for _, input := range []string{
		`{`, `[]`, `{"Format":1}`, `{"Columns":"a"}`,
		`{"Extfs":[]}`, `{"Snapshot_Id":1.5}`, `{"Scan_Mode":true}`,
		`{"format":1,"FORMAT":"parquet"}`,
	} {
		_, err := NormalizeExternalSpec(input)
		require.Error(t, err, input)
	}
	for _, input := range []string{"", "null", "{}"} {
		got, err := NormalizeExternalSpec(input)
		require.NoError(t, err)
		require.Equal(t, input, got)
	}
}

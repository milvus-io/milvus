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

package streamingutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ext "github.com/milvus-io/milvus/pkg/v3/extension"
)

// Delegators live on streaming query nodes exactly when the streaming service
// is on and no form is installed. A form keeps its one streaming node for DDL
// and the write ahead log, and serves queries from regular query nodes.
func TestUseStreamingQueryNodeAsDelegator(t *testing.T) {
	for name, tc := range map[string]struct {
		streaming, form, want bool
	}{
		"stock binary, streaming on":    {streaming: true, form: false, want: true},
		"stock binary, streaming off":   {streaming: false, form: false, want: false},
		"installed form, streaming on":  {streaming: true, form: true, want: false},
		"installed form, streaming off": {streaming: false, form: true, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			ext.ResetForTest()
			t.Cleanup(ext.ResetForTest)
			if tc.form {
				ext.SetForm()
			}
			if tc.streaming {
				t.Setenv(MilvusStreamingServiceEnabled, "1")
			} else {
				t.Setenv(MilvusStreamingServiceEnabled, "")
			}
			assert.Equal(t, tc.want, UseStreamingQueryNodeAsDelegator())
		})
	}
}

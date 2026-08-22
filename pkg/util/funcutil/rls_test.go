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

package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertRLSTemplateVariables(t *testing.T) {
	tests := []struct {
		name           string
		expr           string
		expected       string
		needsPrincipal bool
		tagVariables   map[string]string
	}{
		{
			name:           "convert unquoted variables",
			expr:           "owner == $current_principal and dept == $current_principal_tags['dept'] and backup == $current_principal_tags['dept']",
			expected:       "owner == {__rls_principal} and dept == {__rls_tag_0} and backup == {__rls_tag_0}",
			needsPrincipal: true,
			tagVariables:   map[string]string{"dept": "__rls_tag_0"},
		},
		{
			name:     "preserve quoted variables",
			expr:     `owner == "$current_principal" and marker == "$current_principal_tags['dept']"`,
			expected: `owner == "$current_principal" and marker == "$current_principal_tags['dept']"`,
		},
		{
			name:     "preserve raw and escaped literals",
			expr:     `raw == r"$current_principal" and escaped == "value \"$current_principal\""`,
			expected: `raw == r"$current_principal" and escaped == "value \"$current_principal\""`,
		},
		{
			name:     "preserve longer identifier and malformed tag",
			expr:     "$current_principal_name == owner and dept == $current_principal_tags['']",
			expected: "$current_principal_name == owner and dept == $current_principal_tags['']",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, needsPrincipal, tagVariables := ConvertRLSTemplateVariables(test.expr)
			assert.Equal(t, test.expected, actual)
			assert.Equal(t, test.needsPrincipal, needsPrincipal)
			if test.tagVariables == nil {
				assert.Empty(t, tagVariables)
			} else {
				assert.Equal(t, test.tagVariables, tagVariables)
			}
		})
	}
}

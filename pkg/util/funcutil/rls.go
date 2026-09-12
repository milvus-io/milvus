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
	"fmt"
	"strings"
)

const RLSPrincipalTemplateName = "__rls_principal"

const rlsPrincipalTagPrefix = "$current_principal_tags['"

// ConvertRLSTemplateVariables converts RLS pseudo variables into Milvus
// expression template variables. Pseudo variables inside quoted literals are
// data and therefore follow the same behavior as regular expression templates:
// they are not converted.
func ConvertRLSTemplateVariables(expr string) (string, bool, map[string]string) {
	var builder strings.Builder
	builder.Grow(len(expr))

	needsPrincipal := false
	tagVariables := make(map[string]string)
	var quote byte
	escaped := false

	for i := 0; i < len(expr); {
		ch := expr[i]
		if quote != 0 {
			builder.WriteByte(ch)
			i++
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}

		if ch == '\'' || ch == '"' {
			quote = ch
			builder.WriteByte(ch)
			i++
			continue
		}

		if strings.HasPrefix(expr[i:], rlsPrincipalTagPrefix) {
			keyStart := i + len(rlsPrincipalTagPrefix)
			if keyEndOffset := strings.Index(expr[keyStart:], "']"); keyEndOffset > 0 {
				keyEnd := keyStart + keyEndOffset
				tagKey := expr[keyStart:keyEnd]
				variable, ok := tagVariables[tagKey]
				if !ok {
					variable = fmt.Sprintf("__rls_tag_%d", len(tagVariables))
					tagVariables[tagKey] = variable
				}
				builder.WriteByte('{')
				builder.WriteString(variable)
				builder.WriteByte('}')
				i = keyEnd + len("']")
				continue
			}
		}

		const principal = "$current_principal"
		if strings.HasPrefix(expr[i:], principal) {
			end := i + len(principal)
			if end == len(expr) || !isRLSVariableWordChar(expr[end]) {
				builder.WriteByte('{')
				builder.WriteString(RLSPrincipalTemplateName)
				builder.WriteByte('}')
				needsPrincipal = true
				i = end
				continue
			}
		}

		builder.WriteByte(ch)
		i++
	}

	return builder.String(), needsPrincipal, tagVariables
}

func isRLSVariableWordChar(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

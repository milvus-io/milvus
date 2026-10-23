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

package httpserver

import (
	"github.com/gin-gonic/gin/binding"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// strictValidationEnabled is startup-only so all conversion steps in a request
// use the same policy. Subordinate compatibility knobs cannot enable this path.
func strictValidationEnabled() bool {
	return paramtable.Get().HTTPCfg.StrictValidation.GetAsBool()
}

// ExpressionParams keeps exact literals in strict mode while retaining the old
// interface{} decoder's accepted input domain when the backport is disabled.
type ExpressionParams map[string]json.RawMessage

func (p *ExpressionParams) UnmarshalJSON(data []byte) error {
	if !strictValidationEnabled() {
		var legacy map[string]interface{}
		if err := legacyJSONUnmarshal(data, &legacy); err != nil {
			return err
		}
	}
	return json.Unmarshal(data, (*map[string]json.RawMessage)(p))
}

type SearchIDs []json.RawMessage

func (p *SearchIDs) UnmarshalJSON(data []byte) error {
	if !strictValidationEnabled() {
		var legacy []interface{}
		if err := legacyJSONUnmarshal(data, &legacy); err != nil {
			return err
		}
	}
	return json.Unmarshal(data, (*[]json.RawMessage)(p))
}

// Use Gin's selected JSON decoder in the disabled path, just as the original
// request structs did (including builds with and without the sonic tag).
func legacyJSONUnmarshal(data []byte, target interface{}) error {
	return binding.JSON.BindBody(data, target)
}

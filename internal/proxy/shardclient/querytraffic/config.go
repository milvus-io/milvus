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

package querytraffic

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func ParseRules(raw string) ([]RuleConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	// Reject unknown fields so a typo such as `destinationLabel` or `notIn`
	// fails loudly instead of being silently dropped and compiling into a
	// matcher that matches every candidate.
	decodeStrict := func(v any) error {
		decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
		decoder.DisallowUnknownFields()
		return decoder.Decode(v)
	}

	// Dispatch on the first non-space character so a malformed array input
	// reports the array error instead of a misleading object-parse error.
	switch raw[0] {
	case '[':
		var rules []RuleConfig
		if err := decodeStrict(&rules); err != nil {
			return nil, err
		}
		return rules, nil
	case '{':
		var policy PolicyConfig
		if err := decodeStrict(&policy); err != nil {
			return nil, err
		}
		return policy.Rules, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("rules config must be a JSON array or an object with a rules field, got %q", raw)
	}
}

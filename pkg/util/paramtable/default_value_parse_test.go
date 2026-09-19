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

package paramtable

import (
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// numericLookingDefault matches a DefaultValue that is written in the
// vocabulary of a number: digits, a sign, a decimal point and an exponent
// marker, and nothing else. Values carrying a unit suffix ("32m", "10s") are
// deliberately excluded, because those are read through GetAsSize /
// GetAsDurationByParse, which understand the suffix.
var numericLookingDefault = regexp.MustCompile(`^[-+]?[0-9][0-9eE.+-]*$`)

// nonNumericDefaultValueKeys are the config items whose DefaultValue is made of
// numeric characters but is deliberately NOT a number, so the parse invariant
// below does not apply to them. Both are semantic versions compared with
// semver.Parse, never with a GetAs* numeric accessor.
var nonNumericDefaultValueKeys = map[string]string{
	"dataCoord.channel.legacyVersionWithoutRPCWatch":                "semver, parsed with semver.Parse",
	"dataCoord.compaction.storageVersion.sessionVersionRequirement": "semver, parsed with semver.Parse",
}

// TestParamItemNumericDefaultsAreParseable walks every declared ParamItem and
// asserts that a DefaultValue written like a number really is one.
//
// Every numeric accessor on ParamItem (GetAsInt, GetAsInt64, GetAsUint64,
// GetAsFloat, GetAsDuration, ...) funnels through getAndConvert, which
// swallows the conversion error and substitutes the zero value. A malformed
// numeric default therefore never fails loudly: it silently resolves to 0 in
// every deployment that does not set the key in milvus.yaml -- embedded and
// library use, env-var-only deployments and every unit test. That is how
// rocksmq.lrucacheratio shipped as "0.0.6" (three dots) and zeroed the RocksMQ
// block cache ratio without a single log line.
//
// This test walks the declarations rather than the consumers, so the class
// cannot come back through a config item nobody remembered to test.
func TestParamItemNumericDefaultsAreParseable(t *testing.T) {
	params := newSensitiveAuditParams(t)

	violations := make([]string, 0)
	walkParamItems(reflect.ValueOf(params).Elem(), func(item *ParamItem) {
		value := strings.TrimSpace(item.DefaultValue)
		if value == "" {
			return
		}
		if _, exempt := nonNumericDefaultValueKeys[item.Key]; exempt {
			return
		}
		if !numericLookingDefault.MatchString(value) {
			return
		}
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			violations = append(violations,
				item.Key+" has a numeric-looking DefaultValue "+strconv.Quote(item.DefaultValue)+
					" that does not parse as a number: "+err.Error()+
					" (every GetAs* accessor would silently return 0)")
		}
	})

	if len(violations) > 0 {
		sort.Strings(violations)
		t.Errorf("unparseable numeric DefaultValue(s):\n  %s", strings.Join(violations, "\n  "))
	}
}

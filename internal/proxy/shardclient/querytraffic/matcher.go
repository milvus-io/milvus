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
	"regexp"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type Matcher struct {
	// any marks a matcher compiled from `any: true`, which matches every
	// label set. `any: true` must be used alone (CompileMatcher rejects a
	// combination with other fields), so Match short-circuits on it.
	any       bool
	exists    []string
	notExists []string
	eq        map[string]labelValue
	ne        map[string]labelValue
	in        map[string]map[string]struct{}
	notIn     map[string]map[string]struct{}
	match     map[string]*regexp.Regexp
	notMatch  map[string]*regexp.Regexp
}

type labelValue struct {
	value     string
	sourceKey string
}

func hasMatcherConditions(cfg MatcherConfig) bool {
	return len(cfg.Exists) > 0 ||
		len(cfg.NotExists) > 0 ||
		len(cfg.Eq) > 0 ||
		len(cfg.Ne) > 0 ||
		len(cfg.In) > 0 ||
		len(cfg.NotIn) > 0 ||
		len(cfg.Match) > 0 ||
		len(cfg.NotMatch) > 0
}

func CompileMatcher(cfg MatcherConfig) (*Matcher, error) {
	if cfg.Any && hasMatcherConditions(cfg) {
		return nil, merr.WrapErrParameterInvalidMsg("any: true must be used alone; combining it with other matcher fields is not supported")
	}
	m := &Matcher{
		any:       cfg.Any,
		exists:    append([]string(nil), cfg.Exists...),
		notExists: append([]string(nil), cfg.NotExists...),
	}

	var err error
	if m.eq, err = compileLabelValues(cfg.Eq); err != nil {
		return nil, err
	}
	if m.ne, err = compileLabelValues(cfg.Ne); err != nil {
		return nil, err
	}
	m.in = compileStringSets(cfg.In)
	m.notIn = compileStringSets(cfg.NotIn)
	if m.match, err = compileRegexps(cfg.Match); err != nil {
		return nil, err
	}
	if m.notMatch, err = compileRegexps(cfg.NotMatch); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Matcher) Match(source Labels, target Labels) bool {
	if m == nil || m.any {
		return true
	}
	for _, key := range m.exists {
		if _, ok := target[key]; !ok {
			return false
		}
	}
	for _, key := range m.notExists {
		if _, ok := target[key]; ok {
			return false
		}
	}
	for key, expected := range m.eq {
		actual, ok := target[key]
		resolved, resolvedOK := expected.resolve(source)
		if !ok || !resolvedOK || actual != resolved {
			return false
		}
	}
	// ne matches when the target label either is absent or differs from the
	// expected value: an absent key is not equal to any value. Use not_exists
	// when the intent is to require the key to be missing.
	for key, expected := range m.ne {
		actual, ok := target[key]
		resolved, resolvedOK := expected.resolve(source)
		if !resolvedOK || (ok && actual == resolved) {
			return false
		}
	}
	for key, values := range m.in {
		actual, ok := target[key]
		if !ok {
			return false
		}
		if _, ok := values[actual]; !ok {
			return false
		}
	}
	// not_in matches when the target label is absent (nothing to exclude) or
	// its value is not in the list.
	for key, values := range m.notIn {
		actual, ok := target[key]
		if !ok {
			continue
		}
		if _, ok := values[actual]; ok {
			return false
		}
	}
	for key, re := range m.match {
		actual, ok := target[key]
		if !ok || !re.MatchString(actual) {
			return false
		}
	}
	for key, re := range m.notMatch {
		actual, ok := target[key]
		if ok && re.MatchString(actual) {
			return false
		}
	}
	return true
}

func compileLabelValues(values map[string]string) (map[string]labelValue, error) {
	if len(values) == 0 {
		return nil, nil
	}
	compiled := make(map[string]labelValue, len(values))
	for key, value := range values {
		lv, err := compileLabelValue(value)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "invalid matcher value for label %q", key)
		}
		compiled[key] = lv
	}
	return compiled, nil
}

func compileLabelValue(value string) (labelValue, error) {
	if !strings.Contains(value, "${source.") {
		return labelValue{value: value}, nil
	}
	if !strings.HasPrefix(value, "${source.") || !strings.HasSuffix(value, "}") {
		return labelValue{}, merr.WrapErrParameterInvalidMsg("source label reference must be the whole value")
	}
	sourceKey := strings.TrimSuffix(strings.TrimPrefix(value, "${source."), "}")
	if sourceKey == "" || strings.Contains(sourceKey, "${") {
		return labelValue{}, merr.WrapErrParameterInvalidMsg("invalid source label reference")
	}
	return labelValue{sourceKey: sourceKey}, nil
}

func (v labelValue) resolve(source Labels) (string, bool) {
	if v.sourceKey == "" {
		return v.value, true
	}
	value, ok := source[v.sourceKey]
	return value, ok
}

func compileStringSets(values map[string][]string) map[string]map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	compiled := make(map[string]map[string]struct{}, len(values))
	for key, list := range values {
		set := make(map[string]struct{}, len(list))
		for _, value := range list {
			set[value] = struct{}{}
		}
		compiled[key] = set
	}
	return compiled
}

func compileRegexps(values map[string]string) (map[string]*regexp.Regexp, error) {
	if len(values) == 0 {
		return nil, nil
	}
	compiled := make(map[string]*regexp.Regexp, len(values))
	for key, pattern := range values {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "invalid regex for label %q", key)
		}
		compiled[key] = re
	}
	return compiled, nil
}

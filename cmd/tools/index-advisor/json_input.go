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

package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// decodeInput decodes a JSON input file into v. Keys that v does not declare
// are ignored, so a file with extra data still loads, and are returned as
// warnings, so a misnamed key does not silently read as absent. A warning
// names the key by its path, with [*] for any array element or map entry,
// such as collections[*].fields[*].is_primary.
func decodeInput(data []byte, v any) ([]string, error) {
	if err := json.Unmarshal(data, v); err != nil {
		return nil, err
	}
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	counts := map[string]int{}
	collectUnknownKeys(raw, reflect.TypeOf(v), "", counts)

	paths := make([]string, 0, len(counts))
	for path := range counts {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	warnings := make([]string, 0, len(paths))
	for _, path := range paths {
		w := "ignoring unknown key " + path
		if n := counts[path]; n > 1 {
			w += fmt.Sprintf(" (%d occurrences)", n)
		}
		warnings = append(warnings, w)
	}
	return warnings, nil
}

// collectUnknownKeys walks raw, the generic decoding of a JSON value, along
// t, the type it was decoded into, and counts the object keys that t has no
// field for.
func collectUnknownKeys(raw any, t reflect.Type, path string, counts map[string]int) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		obj, ok := raw.(map[string]any)
		if !ok {
			return
		}
		fields := jsonFields(t)
		for key, value := range obj {
			keyPath := key
			if path != "" {
				keyPath = path + "." + key
			}
			ft, ok := lookupJSONField(fields, key)
			if !ok {
				counts[keyPath]++
				continue
			}
			collectUnknownKeys(value, ft, keyPath, counts)
		}
	case reflect.Slice, reflect.Array:
		list, _ := raw.([]any)
		for _, value := range list {
			collectUnknownKeys(value, t.Elem(), path+"[*]", counts)
		}
	case reflect.Map:
		obj, _ := raw.(map[string]any)
		for _, value := range obj {
			collectUnknownKeys(value, t.Elem(), path+"[*]", counts)
		}
	}
}

// jsonFields returns the JSON key of each field encoding/json decodes into t,
// including the fields of embedded structs.
func jsonFields(t reflect.Type) map[string]reflect.Type {
	fields := map[string]reflect.Type{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		switch {
		case name == "-":
			continue
		case f.Anonymous && name == "" && f.Type.Kind() == reflect.Struct:
			for k, ft := range jsonFields(f.Type) {
				fields[k] = ft
			}
			continue
		case !f.IsExported():
			continue
		case name == "":
			name = f.Name
		}
		fields[name] = f.Type
	}
	return fields
}

// lookupJSONField finds the field key decodes into. Like encoding/json, it
// prefers an exact match and falls back to a case-insensitive one.
func lookupJSONField(fields map[string]reflect.Type, key string) (reflect.Type, bool) {
	if ft, ok := fields[key]; ok {
		return ft, true
	}
	for name, ft := range fields {
		if strings.EqualFold(name, key) {
			return ft, true
		}
	}
	return nil, false
}

// checkFormatVersion reports a missing or unsupported format_version of an
// input file.
func checkFormatVersion(got, want int) error {
	switch got {
	case want:
		return nil
	case 0:
		return merr.WrapErrParameterMissingMsg("format_version is missing; want %d", want)
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported format_version %d, want %d", got, want)
	}
}

// prefixAll prepends prefix to every string in list.
func prefixAll(prefix string, list []string) []string {
	out := make([]string, len(list))
	for i, s := range list {
		out[i] = prefix + s
	}
	return out
}

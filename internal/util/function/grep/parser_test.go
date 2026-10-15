// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grep

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input string
		want  Spec
	}{
		{`-i -e "user_id=[0-9]{3,6}\b.*timeout"`, Spec{Patterns: []string{`user_id=[0-9]{3,6}\b.*timeout`}, CaseMode: CaseInsensitive}},
		{`-Fiea.b --regexp=timeout -e 'a b' -w`, Spec{Patterns: []string{"a.b", "timeout", "a b"}, PatternKind: PatternLiteral, CaseMode: CaseInsensitive, Boundary: BoundaryWord}},
		{`--fixed-strings --ignore-case --case-sensitive --line-regexp --regexp a`, Spec{Patterns: []string{"a"}, PatternKind: PatternLiteral, Boundary: BoundaryLine}},
		{`--word-regexp --regexp=a`, Spec{Patterns: []string{"a"}, Boundary: BoundaryWord}},
		{`-i -s -e a -i`, Spec{Patterns: []string{"a"}, CaseMode: CaseInsensitive}},
		{`-e -C -e--help --regexp=-P`, Spec{Patterns: []string{"-C", "--help", "-P"}}},
		{`-e "" --regexp='' -e a -e a --`, Spec{Patterns: []string{"", "", "a", "a"}}},
		{`-FF -ww -e a`, Spec{Patterns: []string{"a"}, PatternKind: PatternLiteral, Boundary: BoundaryWord}},
		{`-xx -e a`, Spec{Patterns: []string{"a"}, Boundary: BoundaryLine}},
		{`-e=foo`, Spec{Patterns: []string{"=foo"}}},
		{`-e --`, Spec{Patterns: []string{"--"}}},
		{`-e '['`, Spec{Patterns: []string{"["}}}, // Rust, not this parser, validates regex syntax.
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, 65536)
			if err != nil || !reflect.DeepEqual(got, &tt.want) {
				t.Fatalf("got %#v, %v; want %#v", got, err, tt.want)
			}
		})
	}
}

func TestParseRejectsUnsupportedAndMalformedArguments(t *testing.T) {
	for _, input := range []string{
		`-C 2 -e a`, `-C 0 -e a`, `-iC0 -e a`, `-U -e a`, `-P -e a`, `-S -s -e a`,
		`--no-fixed-strings -F -e a`, `--passthrough -e a`, `--regexp-file=file`,
		`-h -e a`, `--help -e a`, `-V -e a`, `--version -e a`, `--unknown -e a`,
		`--reg=a`, `--ignore-case=false -e a`, `--fixed-strings= -e a`,
		`-w -x -e a`, `-x -w -e a`, `-e a file.txt`, `-e a -- -i`, `rg -e a`, `a`, `-`,
		`-e 'abc`, `-e "abc`, "-e abc\\", "-e \x00", "-e \xff", "-e 'a\nb'", "-e a\\\nb",
	} {
		t.Run(input, func(t *testing.T) {
			spec, err := Parse(input, 65536)
			if spec != nil || !errors.Is(err, merr.ErrParameterInvalid) {
				t.Fatalf("got %#v, %v", spec, err)
			}
			status := merr.Status(err)
			if status.Retriable || status.ExtraInfo[merr.InputErrorFlagKey] != "true" {
				t.Fatalf("parse error lost input classification: %v", status)
			}
		})
	}
	for _, input := range []string{"", " \t\n", "--", "-i", "-e", "--regexp", "-Fie"} {
		spec, err := Parse(input, 65536)
		if spec != nil || !errors.Is(err, merr.ErrParameterMissing) {
			t.Fatalf("%q: got %#v, %v", input, spec, err)
		}
	}
}

func TestParseArgumentBytes(t *testing.T) {
	for _, input := range []string{"-e a", "-e 错", `-e ""`} {
		limit := int64(len(input))
		if _, err := Parse(input, limit); err != nil {
			t.Fatal(err)
		}
		spec, err := Parse(input, limit-1)
		if spec != nil || !errors.Is(err, merr.ErrParameterTooLarge) || merr.GetErrorType(err) != merr.InputError {
			t.Fatalf("byte limit case: %#v, %v", spec, err)
		}
		reason := merr.Status(err).GetReason()
		if !strings.Contains(reason, fmt.Sprintf("actual=%d bytes, limit=%d bytes", len(input), limit-1)) {
			t.Fatalf("missing argument byte counts: %s", reason)
		}
	}
	for _, limit := range []int64{0, -1} {
		_, err := Parse("-e a", limit)
		if !errors.Is(err, merr.ErrServiceInternal) || merr.GetErrorType(err) != merr.SystemError {
			t.Fatalf("service config blamed on request: %v", err)
		}
	}
	// More than the former token and pattern caps is allowed within the byte budget.
	input := strings.Repeat("-e a ", 600)
	spec, err := Parse(input, int64(len(input)))
	if err != nil || len(spec.Patterns) != 600 {
		t.Fatalf("unexpected pattern count: %#v, %v", spec, err)
	}
}

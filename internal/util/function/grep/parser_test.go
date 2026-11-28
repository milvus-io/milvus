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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestParse(t *testing.T) {
	tests := []struct {
		input string
		want  Spec
	}{
		{`-i -e "user_id=[0-9]{3,6}\b.*timeout"`, Spec{patterns: []string{`user_id=[0-9]{3,6}\b.*timeout`}, caseMode: CaseInsensitive}},
		{`-Fiea.b --regexp=timeout -e 'a b' -w`, Spec{patterns: []string{"a.b", "timeout", "a b"}, patternKind: PatternLiteral, caseMode: CaseInsensitive, boundary: BoundaryWord}},
		{`--fixed-strings --ignore-case --case-sensitive --line-regexp --regexp a`, Spec{patterns: []string{"a"}, patternKind: PatternLiteral, boundary: BoundaryLine}},
		{`--word-regexp --regexp=a`, Spec{patterns: []string{"a"}, boundary: BoundaryWord}},
		{`-i -s -e a -i`, Spec{patterns: []string{"a"}, caseMode: CaseInsensitive}},
		{`-e -C -e--help --regexp=-P`, Spec{patterns: []string{"-C", "--help", "-P"}}},
		{`-e "" --regexp='' -e a -e a --`, Spec{patterns: []string{"", "", "a", "a"}}},
		{`-FF -ww -e a`, Spec{patterns: []string{"a"}, patternKind: PatternLiteral, boundary: BoundaryWord}},
		{`-xx -e a`, Spec{patterns: []string{"a"}, boundary: BoundaryLine}},
		{`-e=foo`, Spec{patterns: []string{"foo"}}},
		{`-e==foo`, Spec{patterns: []string{"=foo"}}},
		{`-e =foo`, Spec{patterns: []string{"=foo"}}},
		{`-e= -e foo`, Spec{patterns: []string{"", "foo"}}},
		{`-Fie=foo`, Spec{patterns: []string{"foo"}, patternKind: PatternLiteral, caseMode: CaseInsensitive}},
		{`-e --`, Spec{patterns: []string{"--"}}},
		{`-e '['`, Spec{patterns: []string{"["}}}, // Rust, not this parser, validates regex syntax.
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

func TestParseShortEqualsValues(t *testing.T) {
	for _, tc := range []struct{ attached, canonical string }{
		{`-m=1 -e=a`, `-m 1 -e a`},
		{`-m=+0 -e=a`, `-m +0 -e a`},
		{`-im=+2 -e=a`, `-i -m +2 -e a`},
		{`-A=1 -e=a`, `-A 1 -e a`},
		{`-B=+1 -e=a`, `-B +1 -e a`},
		{`-C=2 -e=a`, `-C 2 -e a`},
		{`-iC=+2 -A=0 -e=hit`, `-i -C +2 -A 0 -e hit`},
		{`-C=+0 -C=3 -B=0 -e=a`, `-C +0 -C 3 -B 0 -e a`},
		{`-m=18446744073709551615 -e=a`, `-m 18446744073709551615 -e a`},
		{`-C=+18446744073709551615 -e=a`, `-C +18446744073709551615 -e a`},
	} {
		t.Run(tc.attached, func(t *testing.T) {
			got, err := Parse(tc.attached, 65536)
			if err != nil {
				t.Fatal(err)
			}
			want, err := Parse(tc.canonical, 65536)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v; canonical %#v, %v", got, want, err)
			}
		})
	}
	for _, flag := range []string{"-m", "-A", "-B", "-C"} {
		for _, suffix := range []string{"=", "= 1", "==1", " =1", "=+", "=-1", "=1.5", "=18446744073709551616"} {
			spec, err := Parse(flag+suffix+" -e a", 65536)
			if spec != nil || !errors.Is(err, merr.ErrParameterInvalid) {
				t.Fatalf("%s%s: %#v, %v", flag, suffix, spec, err)
			}
		}
	}
}

func TestParseBoundaryLastOptionWins(t *testing.T) {
	for _, tc := range []struct {
		arguments string
		want      Boundary
	}{
		{`-w -x -e error`, BoundaryLine},
		{`-x -w -e error`, BoundaryWord},
		{`-wx -e error`, BoundaryLine},
		{`-xw -e error`, BoundaryWord},
		{`--word-regexp --line-regexp -e error`, BoundaryLine},
		{`--line-regexp --word-regexp -e error`, BoundaryWord},
		{`-w --line-regexp -e error`, BoundaryLine},
		{`-x --word-regexp -e error`, BoundaryWord},
		{`--word-regexp -Fx -e error`, BoundaryLine},
		{`--line-regexp -Fw -e error`, BoundaryWord},
		{`-xw -e error -xx`, BoundaryLine},
		{`-wx -e error -ww`, BoundaryWord},
	} {
		t.Run(tc.arguments, func(t *testing.T) {
			spec, err := Parse(tc.arguments, 65536)
			if err != nil {
				t.Fatal(err)
			}
			if spec.Boundary() != tc.want {
				t.Fatalf("boundary=%v, want %v", spec.Boundary(), tc.want)
			}
		})
	}
}

func TestParseRejectsUnsupportedAndMalformedArguments(t *testing.T) {
	for _, input := range []string{
		`-U -e a`, `-P -e a`, `-S -s -e a`,
		`--no-fixed-strings -F -e a`, `--passthrough -e a`, `--regexp-file=file`,
		`-h -e a`, `--help -e a`, `-V -e a`, `--version -e a`, `--unknown -e a`,
		`--reg=a`, `--ignore-case=false -e a`, `--fixed-strings= -e a`,
		`-e a file.txt`, `-e a -- -i`, `rg -e a`, `a`, `-`,
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
	if err != nil || spec.PatternCount() != 600 {
		t.Fatalf("unexpected pattern count: %#v, %v", spec, err)
	}
}

func TestParseZeroMaxCountSkipsLiteralLFValidation(t *testing.T) {
	for _, input := range []string{
		"-m0 -e 'a\nb'", "-e 'a\nb' -m0", "--regexp='a\nb' --max-count=+0", "-m=0 -e='a\nb'",
		"-Fm1 -e 'a\nb' -im=+0", "-m0 -m1 -m0 -e 'a\nb'", "-m0 -e ok -e 'a\nb'",
	} {
		spec, err := Parse(input, 65536)
		if err != nil {
			t.Fatalf("%q: %v", input, err)
		}
		if count, present := spec.MaxCount(); !present || count != 0 || spec.Pattern(spec.PatternCount()-1) != "a\nb" {
			t.Fatalf("%q: unexpected spec %#v", input, spec)
		}
	}
	for _, input := range []string{"-e 'a\nb'", "-m1 -e 'a\nb'", "-m0 -e 'a\nb' -m1", "-Fm2 -e 'a\nb'"} {
		spec, err := Parse(input, 65536)
		if spec != nil || !errors.Is(err, merr.ErrParameterInvalid) || !strings.Contains(err.Error(), "literal LF") {
			t.Fatalf("%q: %#v, %v", input, spec, err)
		}
	}
	for _, input := range []string{
		"-m0 -e 'a\nb' --unknown", "-m0 -e 'a\nb' -A -1", "-m0 -e 'a\nb' extra",
		"-m0 -e 'a\nb' --regexp", "-m0 -e \x00", "-m0 -e \xff",
	} {
		if spec, err := Parse(input, 65536); spec != nil || err == nil {
			t.Fatalf("zero cap bypassed argument validation for %q: %#v, %v", input, spec, err)
		}
	}
}

func TestParseMaxCountLeadingPlus(t *testing.T) {
	for _, tc := range []struct {
		arguments string
		want      uint64
	}{
		{`-m +1 -e a`, 1},
		{`-m+1 -e a`, 1},
		{`--max-count +1 -e a`, 1},
		{`--max-count=+1 -e a`, 1},
		{`-im+1 -e a`, 1},
		{`-m+0002 -e a`, 2},
		{`-m+0 -e a`, 0},
		{`--max-count=+0 -e a`, 0},
		{`-m2 -e a -m+1`, 1},
		{`-m+1 -e a -m2`, 2},
		{`-m+18446744073709551615 -e a`, ^uint64(0)},
	} {
		t.Run(tc.arguments, func(t *testing.T) {
			spec, err := Parse(tc.arguments, 65536)
			if err != nil {
				t.Fatal(err)
			}
			if value, present := spec.MaxCount(); !present || value != tc.want {
				t.Fatalf("max-count=%d, present=%v; want %d", value, present, tc.want)
			}
		})
	}
}

func TestParseMaxCount(t *testing.T) {
	for _, input := range []string{`-m 2 -e a`, `-m2 -e a`, `--max-count 2 -e a`, `--max-count=2 -e a`, `-im2 -e a`, `-m0 -e a -m2`, `-m3 --max-count=2 -e a`} {
		spec, err := Parse(input, 65536)
		if err != nil {
			t.Fatal(err)
		}
		if value, present := spec.MaxCount(); !present || value != 2 {
			t.Fatalf("%q: max-count=%d, present=%v", input, value, present)
		}
	}
	for _, input := range []string{`-m0 -e a`, `--max-count=0 -e a`, `-m2 -m0 -e a`} {
		spec, err := Parse(input, 65536)
		if err != nil {
			t.Fatal(err)
		}
		if value, present := spec.MaxCount(); !present || value != 0 {
			t.Fatalf("%q: max-count=%d, present=%v", input, value, present)
		}
	}
	spec, err := Parse(`-e -m`, 65536)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := spec.MaxCount(); present || spec.Pattern(0) != "-m" {
		t.Fatal("pattern value must not be interpreted as a flag")
	}
	spec, err = Parse(`-m18446744073709551615 -e a`, 65536)
	if err != nil {
		t.Fatal(err)
	}
	if value, present := spec.MaxCount(); !present || value != ^uint64(0) {
		t.Fatal("max-count must retain all uint64 bits")
	}
	for _, input := range []string{`-m`, `-e a --max-count`} {
		_, err := Parse(input, 65536)
		if !errors.Is(err, merr.ErrParameterMissing) {
			t.Fatalf("%q: %v", input, err)
		}
	}
	for _, input := range []string{`-m-1 -e a`, `-m + -e a`, `-m++1 -e a`, `-m+-1 -e a`, `-m+1.5 -e a`, `-m+0x10 -e a`, `-m+18446744073709551616 -e a`, `-m2i -e a`, `-m -e a`, `--max-count= -e a`, `-m 1.5 -e a`, `-m0x10 -e a`, `-m1_000 -e a`, `-m18446744073709551616 -e a`, `--max-c=1 -e a`} {
		spec, err := Parse(input, 65536)
		if spec != nil || !errors.Is(err, merr.ErrParameterInvalid) || merr.GetErrorType(err) != merr.InputError {
			t.Fatalf("%q: got %#v, %v", input, spec, err)
		}
		if remote := merr.Error(merr.Status(err)); merr.IsRetryableErr(remote) {
			t.Fatalf("invalid count must not be retried: %v", remote)
		}
	}
}

func TestParseContext(t *testing.T) {
	for _, tc := range []struct {
		args          string
		before, after uint64
	}{
		{"-A2 -e hit", 0, 2},
		{"-B 3 -e hit", 3, 0},
		{"-iC2 -e hit", 2, 2},
		{"--after-context=2 --before-context 3 -e hit", 3, 2},
		{"--context 2 -e hit", 2, 2},
		{"--context=0 -e hit", 0, 0},
		{"-A0 -C3 -e hit", 3, 0},
		{"-C3 -A0 -e hit", 3, 0},
		{"-B1 -C3 -e hit", 1, 3},
		{"-C3 -B1 -e hit", 1, 3},
		{"-A1 -A2 -C3 -C4 -B0 -e hit", 0, 2},
		{"-e -A -e -B -e -C", 0, 0},
		{"-C18446744073709551615 -e hit", ^uint64(0), ^uint64(0)},
		{"-A +1 -e hit", 0, 1},
		{"-B+1 -e hit", 1, 0},
		{"-iC+1 -e hit", 1, 1},
		{"--after-context=+2 --before-context +3 -e hit", 3, 2},
		{"--context +2 -e hit", 2, 2},
		{"--context=+0 -e hit", 0, 0},
		{"-A+0 -C+3 -e hit", 3, 0},
		{"-C+3 -B+0 -e hit", 0, 3},
		{"-B+1 -B+2 -C+3 -C+4 -A+0 -e hit", 2, 0},
		{"-C+0002 -e hit", 2, 2},
		{"-C+18446744073709551615 -e hit", ^uint64(0), ^uint64(0)},
	} {
		t.Run(tc.args, func(t *testing.T) {
			spec, err := Parse(tc.args, 65536)
			if err != nil {
				t.Fatal(err)
			}
			if spec.BeforeContext() != tc.before || spec.AfterContext() != tc.after {
				t.Fatalf("context = (%d,%d), want (%d,%d)", spec.BeforeContext(), spec.AfterContext(), tc.before, tc.after)
			}
		})
	}
	for _, flag := range []string{"-A", "-B", "-C", "--after-context", "--before-context", "--context"} {
		_, err := Parse("-e hit "+flag, 65536)
		if !errors.Is(err, merr.ErrParameterMissing) {
			t.Fatalf("%s: %v", flag, err)
		}
		for _, value := range []string{"-1", "+", "++1", "+-1", "+1.5", "+0x10", "+18446744073709551616", "1.5", "1_000", "0x10", "18446744073709551616", "-e", "''"} {
			spec, err := Parse(flag+" "+value+" -e hit", 65536)
			if spec != nil || !errors.Is(err, merr.ErrParameterInvalid) || merr.IsRetryableErr(merr.Error(merr.Status(err))) {
				t.Fatalf("%s %s: %#v %v", flag, value, spec, err)
			}
		}
	}
}

func TestParseSummaryModes(t *testing.T) {
	for _, tc := range []struct {
		args string
		mode ResultMode
	}{
		{"-c -e hit", ResultCount},
		{"--count -e hit", ResultCount},
		{"-l -e hit", ResultExists},
		{"--files-with-matches -e hit", ResultExists},
		{"-cl -e hit", ResultExists},
		{"-lc -e hit", ResultExists},
		{"--files-with-matches --count -e hit", ResultExists},
		{"-cic -C2 -m1 -e hit", ResultCount},
		{"-lc -m0 -e '['", ResultExists},
		{"-e -l", ResultDetails},
		{"--regexp=-c", ResultDetails},
	} {
		t.Run(tc.args, func(t *testing.T) {
			spec, err := Parse(tc.args, 65536)
			if err != nil {
				t.Fatal(err)
			}
			if spec.ResultMode() != tc.mode {
				t.Fatalf("mode=%v, want %v", spec.ResultMode(), tc.mode)
			}
		})
	}
	for _, args := range []string{"--count=true -e hit", "--files-with-matches=1 -e hit", "-c -C-1 -e hit", "-l -m-1 -e hit", "-l --unknown -e hit", "-c -e 'a\nb'"} {
		if _, err := Parse(args, 65536); err == nil {
			t.Fatalf("accepted invalid arguments %q", args)
		}
	}
}

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
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func FuzzParse(f *testing.F) {
	for _, seed := range []string{`-e ""`, `-Fiea.b`, `-i -e "user_id=\d+\b"`, `-e 'a\b'`, "-e \xff", `-w -x -e a`, `-e --`, "-e a\\"} {
		f.Add(seed)
	}
	f.Add("-m0 -e 'a\nb'")
	f.Add("-m1 -C+2 -e hit")
	f.Fuzz(func(t *testing.T, input string) {
		const maxArgumentBytes int64 = 4096
		spec, err := Parse(input, maxArgumentBytes)
		if err != nil {
			if spec != nil {
				t.Fatal("partial result on failure")
			}
			return
		}
		if spec == nil || spec.PatternCount() == 0 {
			t.Fatal("invalid successful result")
		}
		var args []string
		count, capped := spec.MaxCount()
		if capped {
			args = append(args, "-m", strconv.FormatUint(count, 10))
		}
		if before := spec.BeforeContext(); before != 0 {
			args = append(args, "-B", strconv.FormatUint(before, 10))
		}
		if after := spec.AfterContext(); after != 0 {
			args = append(args, "-A", strconv.FormatUint(after, 10))
		}
		if spec.PatternKind() == PatternLiteral {
			args = append(args, "-F")
		}
		if spec.CaseMode() == CaseInsensitive {
			args = append(args, "-i")
		}
		switch spec.Boundary() {
		case BoundaryWord:
			args = append(args, "-w")
		case BoundaryLine:
			args = append(args, "-x")
		}
		total := 0
		for i := 0; i < spec.PatternCount(); i++ {
			p := spec.Pattern(i)
			if !utf8.ValidString(p) || strings.ContainsRune(p, '\x00') ||
				(strings.ContainsRune(p, '\n') && (!capped || count != 0)) {
				t.Fatal("invalid normalized pattern")
			}
			total += len(p)
			quoted := strings.ReplaceAll(strings.ReplaceAll(p, `\`, `\\`), `"`, `\"`)
			args = append(args, "-e", `"`+quoted+`"`)
		}
		if total > len(input) {
			t.Fatal("patterns expanded beyond arguments")
		}
		canonical := strings.Join(args, " ")
		roundtrip, err := Parse(canonical, int64(len(canonical)))
		if err != nil || !reflect.DeepEqual(spec, roundtrip) {
			t.Fatalf("round trip failed for %q: %v", canonical, err)
		}
	})
}

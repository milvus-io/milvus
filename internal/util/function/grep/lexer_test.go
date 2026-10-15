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
	"strings"
	"testing"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{`-i -e "user_id=[0-9]{3,6}\b.*timeout"`, []string{"-i", "-e", `user_id=[0-9]{3,6}\b.*timeout`}},
		{`-e 'a\b "c"'`, []string{"-e", `a\b "c"`}},
		{`-e "a\b\d\n\"\\"`, []string{"-e", "a\\b\\d\\n\"\\"}},
		{`-e a\ b\'c\"d\\e`, []string{"-e", `a b'c"d\e`}},
		{`-e ab' c'"d"ef`, []string{"-e", "ab cdef"}},
		{`-e "" -e ''`, []string{"-e", "", "-e", ""}},
		{"\t-i\r\n-e\t'错误：error' ", []string{"-i", "-e", "错误：error"}},
		{"-e a\u00a0b", []string{"-e", "a\u00a0b"}},
		{`-e '$HOME $(command) * # ~'`, []string{"-e", "$HOME $(command) * # ~"}},
		{"-e a\\\nb", []string{"-e", "a\nb"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tokens, err := tokenize(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			var values []string
			for _, token := range tokens {
				values = append(values, token.value)
			}
			if !reflect.DeepEqual(values, tt.want) {
				t.Fatalf("got %q, want %q", values, tt.want)
			}
		})
	}
}

func TestTokenLocationsAndLexicalFailures(t *testing.T) {
	tokens, err := tokenize(`  -e "a b"`)
	if err != nil || tokens[0].start != 2 || tokens[1].start != 5 {
		t.Fatalf("tokens=%v err=%v", tokens, err)
	}
	for _, input := range []string{`-e 'abc`, `-e "abc`, "-e abc\\", "-e \x00", "-e \xff"} {
		tokens, err := tokenize(input)
		if err == nil || tokens != nil {
			t.Fatalf("accepted %q or returned partial tokens", input)
		}
	}
	_, err = tokenize(`-e "unterminated`)
	if err == nil || !strings.Contains(err.Error(), "byte 3") {
		t.Fatalf("wrong quote location: %v", err)
	}
}

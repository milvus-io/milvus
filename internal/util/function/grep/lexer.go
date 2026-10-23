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
	"strings"
	"unicode/utf8"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type token struct {
	value string
	start int // byte offset of the original token, including opening quotes
}

func separator(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// tokenize processes an already transport-decoded UTF-8 string. It is not a
// shell lexer: only the documented quoting/escaping rules are implemented.
func tokenize(input string) ([]token, error) {
	if !utf8.ValidString(input) {
		return nil, merr.WrapErrParameterInvalidMsg("grep arguments must be valid UTF-8")
	}
	if strings.IndexByte(input, 0) >= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("grep arguments must not contain NUL")
	}
	var tokens []token
	var value strings.Builder
	active, start, quoteAt := false, 0, 0
	var quote byte
	finish := func() {
		if active {
			tokens = append(tokens, token{value.String(), start})
			value.Reset()
			active = false
		}
	}
	for i := 0; i < len(input); {
		b := input[i]
		if quote == 0 && separator(b) {
			finish()
			i++
			continue
		}
		if !active {
			active, start = true, i
		}
		if quote == '\'' {
			if b == '\'' {
				quote = 0
			} else {
				value.WriteByte(b)
			}
			i++
			continue
		}
		if quote == '"' {
			switch {
			case b == '"':
				quote = 0
				i++
			case b == '\\' && i+1 < len(input) && (input[i+1] == '"' || input[i+1] == '\\'):
				value.WriteByte(input[i+1])
				i += 2
			default:
				// In particular, preserve regex escapes such as \b and \d.
				value.WriteByte(b)
				i++
			}
			continue
		}
		switch b {
		case '\'', '"':
			quote, quoteAt = b, i
			i++
		case '\\':
			if i+1 == len(input) {
				return nil, merr.WrapErrParameterInvalidMsg("dangling escape in grep arguments at byte %d", i)
			}
			next := input[i+1]
			if separator(next) || next == '\'' || next == '"' || next == '\\' {
				value.WriteByte(next)
				i += 2
			} else {
				value.WriteByte(b)
				i++
			}
		default:
			value.WriteByte(b)
			i++
		}
	}
	if quote != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("unclosed quote in grep arguments at byte %d", quoteAt)
	}
	finish()
	return tokens, nil
}

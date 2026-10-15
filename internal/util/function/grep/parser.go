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

// Parse accepts the documented phase-one rg argument string, without an
// executable name or positional patterns/paths. It returns no partial Spec on
// failure. Regex syntax is deliberately left to the actual Rust engine.
func Parse(arguments string, maxArgumentBytes int64) (*Spec, error) {
	if maxArgumentBytes <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("grep argument byte limit must be positive")
	}
	if int64(len(arguments)) > maxArgumentBytes {
		return nil, merr.Wrapf(merr.WrapErrParameterTooLarge("grep argument bytes"),
			"grep arguments too large: actual=%d bytes, limit=%d bytes", len(arguments), maxArgumentBytes)
	}
	tokens, err := tokenize(arguments)
	if err != nil {
		return nil, err
	}
	spec := &Spec{}
	appendPattern := func(value string) error {
		if strings.IndexByte(value, '\n') >= 0 {
			return merr.WrapErrParameterInvalidMsg("grep patterns must not contain literal LF")
		}
		spec.Patterns = append(spec.Patterns, value)
		return nil
	}
	setBoundary := func(boundary Boundary) error {
		if spec.Boundary != BoundaryNone && spec.Boundary != boundary {
			return merr.WrapErrParameterInvalidMsg("grep options -w and -x cannot be combined")
		}
		spec.Boundary = boundary
		return nil
	}
	for i := 0; i < len(tokens); i++ {
		t := tokens[i]
		value := t.value
		if value == "--" {
			if i+1 != len(tokens) {
				return nil, merr.WrapErrParameterInvalidMsg("grep does not accept positional arguments after --")
			}
			break
		}
		consumePattern := func() error {
			if i+1 >= len(tokens) {
				return merr.WrapErrParameterMissingMsg("grep option %s requires a pattern", value)
			}
			i++ // Consume even an option-looking value such as -C or --.
			return appendPattern(tokens[i].value)
		}
		if strings.HasPrefix(value, "--") {
			name, attached, hasValue := strings.Cut(value, "=")
			if name == "--regexp" {
				if hasValue {
					err = appendPattern(attached)
				} else {
					err = consumePattern()
				}
				if err != nil {
					return nil, err
				}
				continue
			}
			switch name {
			case "--fixed-strings", "--ignore-case", "--case-sensitive", "--word-regexp", "--line-regexp":
				if hasValue {
					return nil, merr.WrapErrParameterInvalidMsg("grep option %s does not accept a value", name)
				}
			default:
				return nil, merr.WrapErrParameterInvalidMsg("unsupported grep option %q at token starting byte %d", name, t.start)
			}
			switch name {
			case "--fixed-strings":
				spec.PatternKind = PatternLiteral
			case "--ignore-case":
				spec.CaseMode = CaseInsensitive
			case "--case-sensitive":
				spec.CaseMode = CaseSensitive
			case "--word-regexp":
				err = setBoundary(BoundaryWord)
			case "--line-regexp":
				err = setBoundary(BoundaryLine)
			}
			if err != nil {
				return nil, err
			}
			continue
		}
		if len(value) < 2 || value[0] != '-' {
			return nil, merr.WrapErrParameterInvalidMsg("grep requires -e/--regexp; positional token at byte %d is not supported", t.start)
		}
		for j := 1; j < len(value); j++ {
			switch value[j] {
			case 'F':
				spec.PatternKind = PatternLiteral
			case 'i':
				spec.CaseMode = CaseInsensitive
			case 's':
				spec.CaseMode = CaseSensitive
			case 'w':
				err = setBoundary(BoundaryWord)
			case 'x':
				err = setBoundary(BoundaryLine)
			case 'e':
				if j+1 < len(value) {
					err = appendPattern(value[j+1:])
				} else {
					err = consumePattern()
				}
				j = len(value) // -e consumes the remainder, not more flags.
			default:
				flag, _ := utf8.DecodeRuneInString(value[j:])
				return nil, merr.WrapErrParameterInvalidMsg("unsupported grep option -%c at token starting byte %d", flag, t.start)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	if len(spec.Patterns) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("grep requires at least one -e/--regexp pattern")
	}
	return spec, nil
}

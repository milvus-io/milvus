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
	"strconv"
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
	var before, after *uint64
	var contextLines uint64
	setContext := func(flag byte, value string) error {
		count, err := strconv.ParseUint(strings.TrimPrefix(value, "+"), 10, 64)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("grep context count must be an unsigned decimal integer in [0, 18446744073709551615], got %q", value)
		}
		switch flag {
		case 'A':
			after = &count
		case 'B':
			before = &count
		case 'C':
			contextLines = count
		}
		return nil
	}
	setMaxCount := func(value string) error {
		// rg accepts one optional leading plus sign for its unsigned count.
		count, err := strconv.ParseUint(strings.TrimPrefix(value, "+"), 10, 64)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("grep max-count must be an unsigned decimal integer in [0, 18446744073709551615], got %q", value)
		}
		spec.maxCount = &count
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
			spec.patterns = append(spec.patterns, tokens[i].value)
			return nil
		}
		if strings.HasPrefix(value, "--") {
			// Value-taking flags consume the next token even if it resembles an option.
			name, attached, hasValue := strings.Cut(value, "=")
			if name == "--after-context" || name == "--before-context" || name == "--context" {
				if !hasValue {
					if i+1 >= len(tokens) {
						return nil, merr.WrapErrParameterMissingMsg("grep option %s requires a count", name)
					}
					i++
					attached = tokens[i].value
				}
				flag := byte('C')
				switch name {
				case "--after-context":
					flag = 'A'
				case "--before-context":
					flag = 'B'
				}
				if err := setContext(flag, attached); err != nil {
					return nil, err
				}
				continue
			}
			if name == "--max-count" {
				if !hasValue {
					if i+1 >= len(tokens) {
						return nil, merr.WrapErrParameterMissingMsg("grep option %s requires a count", name)
					}
					i++
					attached = tokens[i].value
				}
				if err := setMaxCount(attached); err != nil {
					return nil, err
				}
				continue
			}
			if name == "--regexp" {
				if hasValue {
					spec.patterns = append(spec.patterns, attached)
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
				spec.patternKind = PatternLiteral
			case "--ignore-case":
				spec.caseMode = CaseInsensitive
			case "--case-sensitive":
				spec.caseMode = CaseSensitive
			case "--word-regexp":
				spec.boundary = BoundaryWord
			case "--line-regexp":
				spec.boundary = BoundaryLine
			}
			continue
		}
		if len(value) < 2 || value[0] != '-' {
			return nil, merr.WrapErrParameterInvalidMsg("grep requires -e/--regexp; positional token at byte %d is not supported", t.start)
		}
		for j := 1; j < len(value); j++ {
			switch value[j] {
			case 'A', 'B', 'C':
				count := value[j+1:]
				if count == "" {
					if i+1 >= len(tokens) {
						return nil, merr.WrapErrParameterMissingMsg("grep option -%c requires a count", value[j])
					}
					i++
					count = tokens[i].value
				} else {
					count = strings.TrimPrefix(count, "=")
				}
				err = setContext(value[j], count)
				j = len(value)
			case 'F':
				spec.patternKind = PatternLiteral
			case 'i':
				spec.caseMode = CaseInsensitive
			case 's':
				spec.caseMode = CaseSensitive
			case 'w':
				spec.boundary = BoundaryWord
			case 'x':
				spec.boundary = BoundaryLine
			case 'e':
				if j+1 < len(value) {
					spec.patterns = append(spec.patterns, strings.TrimPrefix(value[j+1:], "="))
				} else {
					err = consumePattern()
				}
				j = len(value) // -e consumes the remainder, not more flags.
			case 'm':
				count := value[j+1:]
				if count == "" {
					if i+1 >= len(tokens) {
						return nil, merr.WrapErrParameterMissingMsg("grep option -m requires a count")
					}
					i++
					count = tokens[i].value
				} else {
					count = strings.TrimPrefix(count, "=")
				}
				err = setMaxCount(count)
				j = len(value)
			default:
				flag, _ := utf8.DecodeRuneInString(value[j:])
				return nil, merr.WrapErrParameterInvalidMsg("unsupported grep option -%c at token starting byte %d", flag, t.start)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	if len(spec.patterns) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("grep requires at least one -e/--regexp pattern")
	}
	// rg skips pattern validation for a final -m 0. Parse every option first
	// so later overrides and malformed arguments still take effect.
	if spec.maxCount == nil || *spec.maxCount != 0 {
		for _, pattern := range spec.patterns {
			if strings.IndexByte(pattern, '\n') >= 0 {
				return nil, merr.WrapErrParameterInvalidMsg("grep patterns must not contain literal LF")
			}
		}
	}
	// Explicit -A/-B override their side of -C regardless of argument order,
	// including an explicitly supplied zero (ripgrep context precedence).
	spec.beforeContext, spec.afterContext = contextLines, contextLines
	if before != nil {
		spec.beforeContext = *before
	}
	if after != nil {
		spec.afterContext = *after
	}
	return spec, nil
}

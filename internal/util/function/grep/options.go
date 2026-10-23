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

// Package grep parses the supported rg-style argument subset without cgo,
// shell execution or regex compilation. The execution adapter owns engine validation.
package grep

type PatternKind uint32

const (
	PatternRegex PatternKind = iota
	PatternLiteral
)

type CaseMode uint32

const (
	CaseSensitive CaseMode = iota
	CaseInsensitive
)

type Boundary uint32

const (
	BoundaryNone Boundary = iota
	BoundaryWord
	BoundaryLine
)

// Spec is immutable matching configuration produced by Parse. Patterns retain
// their order and are combined with OR by the Rust engine. Its zero value is
// not a valid parsed specification. Field identity and budgets belong to the
// request, not this value.
type Spec struct {
	patterns    []string
	patternKind PatternKind
	caseMode    CaseMode
	boundary    Boundary
}

// PatternCount returns the number of parsed patterns.
func (s Spec) PatternCount() int { return len(s.patterns) }

// Pattern returns the pattern at index. The index must be in [0, PatternCount()).
// Returning individual strings keeps the backing slice private.
func (s Spec) Pattern(index int) string { return s.patterns[index] }

func (s Spec) PatternKind() PatternKind { return s.patternKind }
func (s Spec) CaseMode() CaseMode       { return s.caseMode }
func (s Spec) Boundary() Boundary       { return s.boundary }

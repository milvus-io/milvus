//go:build go1.20 && !go1.26
// +build go1.20,!go1.26

/*
 * Copyright 2022 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package internal

import "strings"

type Selector interface {
	Match(info *funcInfo) bool
}

const (
	CTAnd CombineType = 0
	CTOr  CombineType = 1
)

type CombineType int

func NewCombinedSelector(combineType CombineType, selectors ...Selector) *CombinedSelector {
	return &CombinedSelector{
		combineType: combineType,
		selectors:   selectors,
		inverse:     false,
	}
}

type CombinedSelector struct {
	combineType CombineType
	selectors   []Selector
	inverse     bool
}

func (s *CombinedSelector) Match(info *funcInfo) (res bool) {
	defer func() {
		if s.inverse {
			res = !res
		}
	}()
	switch s.combineType {
	case CTAnd:
		for _, sel := range s.selectors {
			if !sel.Match(info) {
				return false
			}
		}
		return true
	case CTOr:
		for _, sel := range s.selectors {
			if sel.Match(info) {
				return true
			}
		}
		return false
	default:
		panic("not here")
	}
}

func (s *CombinedSelector) Add(selectors ...Selector) {
	s.selectors = append(s.selectors, selectors...)
}

func (s *CombinedSelector) Not() {
	s.inverse = true
}

const (
	MMExact   MatchMode = 0
	MMContain MatchMode = 1
)

type MatchMode int

func (m MatchMode) match(a, b string) bool {
	switch m {
	case MMExact:
		return a == b
	case MMContain:
		return strings.Contains(a, b)
	default:
		panic("not here")
	}
}

func NewPkgSelector(name string, mode MatchMode) PkgSelector {
	return PkgSelector{
		mode: mode,
		name: name,
	}
}

type PkgSelector struct {
	mode MatchMode
	name string
}

func (s PkgSelector) Match(info *funcInfo) bool {
	return s.mode.match(info.Analyzer.PkgName(), s.name)
}

func NewTypeSelector(name string, mode MatchMode) TypeSelector {
	return TypeSelector{
		mode: mode,
		name: name,
	}
}

type TypeSelector struct {
	mode MatchMode
	name string
}

func (s TypeSelector) Match(info *funcInfo) bool {
	typeName := info.Analyzer.MiddleName()
	typeName = strings.TrimPrefix(typeName, "(*")
	typeName = strings.TrimSuffix(typeName, ")")
	return s.mode.match(typeName, s.name)
}

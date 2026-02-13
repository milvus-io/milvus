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

package iface

import (
	"github.com/bytedance/mockey/exp/iface/internal"
	"github.com/bytedance/mockey/internal/tool"
)

type option struct {
	selector *internal.CombinedSelector
}

type OptionFn func(*option)

func resolveOpt(fn ...OptionFn) *option {
	opt := &option{
		selector: internal.NewCombinedSelector(internal.CTAnd),
	}
	for _, f := range fn {
		f(opt)
	}
	return opt
}

// SelectPkg select the package name exactly match the given names, such as "github.com/bytedance/mockey"
func SelectPkg(names ...string) OptionFn {
	tool.Assert(len(names) > 0, "SelectPkg: at least one package name is required")
	return func(opt *option) {
		s := internal.NewCombinedSelector(internal.CTOr)
		for _, name := range names {
			s.Add(internal.NewPkgSelector(name, internal.MMExact))
		}
		opt.selector.Add(s)
	}
}

// SelectType select the type name exactly match the given names, such as "MockBuilder"
func SelectType(names ...string) OptionFn {
	tool.Assert(len(names) > 0, "SelectType: at least one type name is required")
	return func(opt *option) {
		s := internal.NewCombinedSelector(internal.CTOr)
		for _, name := range names {
			s.Add(internal.NewTypeSelector(name, internal.MMExact))
		}
		opt.selector.Add(s)
	}
}

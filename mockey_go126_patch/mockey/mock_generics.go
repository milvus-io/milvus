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

package mockey

import "github.com/bytedance/mockey/internal/fn"

// MockGeneric mocks generic function. Target must be generic method or method of generic types
// From go1.20, Mock can automatically judge whether the target is generic or not.
func MockGeneric(target interface{}, opt ...mockOptionFn) *MockBuilder {
	return Mock(target, append(opt, OptGeneric)...)
}

type GenericInfo = fn.GenericInfo

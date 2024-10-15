// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package row

import (
	"go/ast"
	"reflect"
)

func ParseCandidate(dataType reflect.Type) map[string]int {
	result := make(map[string]int)
	for i := 0; i < dataType.NumField(); i++ {
		f := dataType.Field(i)
		// ignore anonymous field for now
		if f.Anonymous || !ast.IsExported(f.Name) {
			continue
		}

		name := f.Name
		tag := f.Tag.Get(MilvusTag)
		tagSettings := ParseTagSetting(tag, MilvusTagSep)
		if tagName, has := tagSettings[MilvusTagName]; has {
			name = tagName
		}

		result[name] = i
	}
	return result
}

// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_RandomString(t *testing.T) {
	length := 10
	str := RandomString(length)
	assert.Equal(t, len(str), length)
}

func Test_GenRandomStr(t *testing.T) {
	assert.True(t, len(GenRandomStr()) >= 1)
}

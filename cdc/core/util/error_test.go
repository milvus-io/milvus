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

package util

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnRecoverableError(t *testing.T) {
	uerror := Unrecoverable(errors.New("unrecoverable error"))
	assert.True(t, IsUnRecoverable(uerror))
	assert.False(t, IsUnRecoverable(errors.New("other error")))
}

func TestErrorList(t *testing.T) {
	errorList := ErrorList{}
	errorList = append(errorList, errors.New("line 1"), errors.New("line 2"))
	assert.Equal(t, 3, strings.Count(errorList.Error(), "\n"))

	errorList = append(errorList, nil, errors.New("line 3"))
	assert.Equal(t, 3, strings.Count(errorList.Error(), "\n"))
}

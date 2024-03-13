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

package info

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSdkTypeByUserAgent(t *testing.T) {
	_, ok := getSdkTypeByUserAgent([]string{})
	assert.False(t, ok)

	sdk, ok := getSdkTypeByUserAgent([]string{"grpc-node-js.test"})
	assert.True(t, ok)
	assert.Equal(t, "nodejs", sdk)

	sdk, ok = getSdkTypeByUserAgent([]string{"grpc-python.test"})
	assert.True(t, ok)
	assert.Equal(t, "Python", sdk)

	sdk, ok = getSdkTypeByUserAgent([]string{"grpc-go.test"})
	assert.True(t, ok)
	assert.Equal(t, "Golang", sdk)

	sdk, ok = getSdkTypeByUserAgent([]string{"grpc-java.test"})
	assert.True(t, ok)
	assert.Equal(t, "Java", sdk)

	_, ok = getSdkTypeByUserAgent([]string{"invalid_type"})
	assert.False(t, ok)
}

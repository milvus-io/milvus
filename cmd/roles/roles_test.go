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

package roles

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoles(t *testing.T) {
	r := MilvusRoles{}

	assert.True(t, r.EnvValue("1"))
	assert.True(t, r.EnvValue(" 1 "))
	assert.True(t, r.EnvValue("True"))
	assert.True(t, r.EnvValue(" True "))
	assert.True(t, r.EnvValue(" TRue "))
	assert.False(t, r.EnvValue("0"))
	assert.False(t, r.EnvValue(" 0 "))
	assert.False(t, r.EnvValue(" false "))
	assert.False(t, r.EnvValue(" False "))
	assert.False(t, r.EnvValue(" abc "))

	ss := strings.SplitN("abcdef", "=", 2)
	assert.Equal(t, len(ss), 1)
	ss = strings.SplitN("adb=def", "=", 2)
	assert.Equal(t, len(ss), 2)
}

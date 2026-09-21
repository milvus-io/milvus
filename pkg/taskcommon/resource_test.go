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

package taskcommon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResource(t *testing.T) {
	assert.True(t, Resource{}.IsZero())
	assert.False(t, Resource{CPU: 1}.IsZero())
	assert.False(t, Resource{Memory: 1}.IsZero())

	sum := Resource{CPU: 1, Memory: 10}.Add(Resource{CPU: 2, Memory: 20})
	assert.Equal(t, Resource{CPU: 3, Memory: 30}, sum)

	diff := Resource{CPU: 3, Memory: 30}.Sub(Resource{CPU: 1, Memory: 10})
	assert.Equal(t, Resource{CPU: 2, Memory: 20}, diff)

	// Sub never goes negative: a release that exceeds what was booked clamps to zero.
	clamped := Resource{CPU: 1, Memory: 10}.Sub(Resource{CPU: 5, Memory: 50})
	assert.Equal(t, Resource{}, clamped)

	assert.Equal(t, "cpu=2 memory=20", Resource{CPU: 2, Memory: 20}.String())
}

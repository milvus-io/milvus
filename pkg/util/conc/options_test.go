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

package conc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolOption(t *testing.T) {
	opt := &poolOption{}

	o := WithPreAlloc(true)
	o(opt)
	assert.True(t, opt.preAlloc)

	o = WithNonBlocking(true)
	o(opt)
	assert.True(t, opt.nonBlocking)

	o = WithDisablePurge(true)
	o(opt)
	assert.True(t, opt.disablePurge)

	o = WithExpiryDuration(time.Second)
	o(opt)
	assert.Equal(t, time.Second, opt.expiryDuration)

	o = WithConcealPanic(true)
	o(opt)
	assert.True(t, opt.concealPanic)
}

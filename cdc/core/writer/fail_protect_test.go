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

package writer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestErrorProtect(t *testing.T) {
	protect := FastFail()
	protect.Inc()
	select {
	case <-protect.Chan():
	case <-time.Tick(time.Second):
		assert.Fail(t, "should trigger err protect")
	}

	protect = NewErrorProtect(10, time.Second)
	assert.Contains(t, protect.Info(), "per: 10")
	go func() {
		for i := 0; i < 5; i++ {
			protect.Inc()
		}
	}()
	select {
	case <-protect.Chan():
		assert.Fail(t, "should trigger err protect")
	case <-time.Tick(1500 * time.Millisecond):
	}
	assert.Equal(t, int32(0), protect.current.Load())
	go func() {
		for i := 0; i < 20; i++ {
			protect.Inc()
		}
	}()
	select {
	case <-protect.Chan():
	case <-time.Tick(time.Second):
		assert.Fail(t, "should trigger err protect")
	}
}

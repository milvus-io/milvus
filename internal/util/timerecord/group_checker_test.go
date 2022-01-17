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

package timerecord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGroupChecker(t *testing.T) {
	groupName := `test_group`
	signal := make(chan []string, 1)
	gc1 := GetGroupChecker(groupName, 10*time.Millisecond, func(list []string) {
		signal <- list
	})
	gc1.Check("1")
	gc2 := GetGroupChecker(groupName, time.Second, func(list []string) {
		t.FailNow()
	})
	gc2.Check("2")

	assert.Equal(t, 10*time.Millisecond, gc2.d)

	list := <-signal
	assert.ElementsMatch(t, []string{"1", "2"}, list)

	gc2.Remove("2")

	list = <-signal
	assert.ElementsMatch(t, []string{"1"}, list)

	assert.NotPanics(t, func() {
		gc1.Stop()
		gc2.Stop()
	})
}

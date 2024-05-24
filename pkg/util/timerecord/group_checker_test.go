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

func TestChecker(t *testing.T) {
	groupName := `test_group`
	signal := make(chan []string, 1)
	// 10ms period which set before is too short
	// change 10ms to 500ms to ensure the group checker schedule after the second value stored
	duration := 500 * time.Millisecond
	gc1 := GetCheckerManger(groupName, duration, func(list []string) {
		signal <- list
	})

	checker1 := NewChecker("1", gc1)
	checker1.Check()

	gc2 := GetCheckerManger(groupName, time.Second, func(list []string) {
		t.FailNow()
	})
	checker2 := NewChecker("2", gc2)
	checker2.Check()

	assert.Equal(t, duration, gc2.d)

	assert.Eventually(t, func() bool {
		list := <-signal
		return len(list) == 2
	}, duration*3, duration)

	checker2.Close()
	list := <-signal
	assert.ElementsMatch(t, []string{"1"}, list)

	checker1.Close()

	assert.NotPanics(t, func() {
		gc1.Stop()
		gc2.Stop()
	})
}

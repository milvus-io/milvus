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

package datanode

import (
	"context"
	"testing"
)

func TestCompactionExecutor(t *testing.T) {
	t.Run("Test execute", func(t *testing.T) {
		ex := newCompactionExecutor()
		go ex.start(context.TODO())
		ex.execute(newMockCompactor(true))
	})

	t.Run("Test start", func(t *testing.T) {
		ex := newCompactionExecutor()
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		go ex.start(ctx)
	})

	t.Run("Test excuteTask", func(t *testing.T) {
		tests := []struct {
			isvalid bool

			description string
		}{
			{true, "compact return nil"},
			{false, "compact return error"},
		}

		ex := newCompactionExecutor()
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {
					ex.executeTask(newMockCompactor(true))
				} else {
					ex.executeTask(newMockCompactor(false))
				}
			})
		}
	})

}

func newMockCompactor(isvalid bool) compactor {
	return &mockCompactor{isvalid}
}

type mockCompactor struct {
	isvalid bool
}

func (mc *mockCompactor) compact() error {
	if mc.isvalid {
		return errStart
	}
	return nil
}

func (mc *mockCompactor) getPlanID() UniqueID {
	return 1
}

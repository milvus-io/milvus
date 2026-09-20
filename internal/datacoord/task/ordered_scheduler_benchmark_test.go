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

package task

import (
	"context"
	"strconv"
	"testing"
)

// BenchmarkGlobalSchedulerPendingRound measures the recurring scheduling cost
// of a backlog when no worker can accept it. No worker RPCs are involved.
func BenchmarkGlobalSchedulerPendingRound(b *testing.B) {
	for _, count := range []int{100, 10000, 100000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			scheduler := NewGlobalTaskScheduler(context.Background(), &schedulerTestCluster{}).(*globalTaskScheduler)
			b.Cleanup(scheduler.Stop)
			for id := count; id > 0; id-- {
				ownTask(scheduler, newSchedulerTestTask(int64(id)))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				scheduler.round()
			}
		})
	}
}

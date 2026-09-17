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

import "fmt"

// Resource is what one task is expected to occupy on a worker for its whole
// run, or what a worker has in total / has left. CPU is whole cores; Memory is
// bytes. It is estimated only by DataCoord; a worker only adds and subtracts it.
type Resource struct {
	CPU    int64
	Memory int64
}

func (r Resource) IsZero() bool {
	return r.CPU == 0 && r.Memory == 0
}

func (r Resource) Add(o Resource) Resource {
	return Resource{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory}
}

// Sub subtracts o and clamps each dimension at zero, so a release that exceeds
// what was booked (a request that changed mid-flight) cannot drive the ledger
// negative.
func (r Resource) Sub(o Resource) Resource {
	return Resource{CPU: max(r.CPU-o.CPU, 0), Memory: max(r.Memory-o.Memory, 0)}
}

func (r Resource) String() string {
	return fmt.Sprintf("cpu=%d memory=%d", r.CPU, r.Memory)
}

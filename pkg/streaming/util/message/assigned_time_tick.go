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

package message

import "fmt"

// AssignedTimeTick returns the time tick of msg and true, or 0 and false when
// no time tick is assigned to msg yet. TimeTick panics in the second case.
func AssignedTimeTick(msg BasicMessage) (uint64, bool) {
	value, ok := msg.Properties().Get(messageTimeTick)
	if !ok {
		return 0, false
	}
	tt, err := DecodeUint64(value)
	if err != nil {
		panic(fmt.Sprintf("there's a bug in the message codes, dirty timetick %s in properties of message", value))
	}
	return tt, true
}

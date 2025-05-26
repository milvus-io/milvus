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

import "time"

type Times struct {
	queueTime time.Time
	startTime time.Time
	endTime   time.Time
}

func NewTimes() *Times {
	return &Times{
		queueTime: time.Now(),
		startTime: time.Now(),
		endTime:   time.Now(),
	}
}

func (t *Times) SetTaskTime(timeType TimeType, time time.Time) {
	switch timeType {
	case TimeQueue:
		t.queueTime = time
	case TimeStart:
		t.startTime = time
	case TimeEnd:
		t.endTime = time
	}
}

type TimeType string

const (
	TimeQueue TimeType = "QueueTime"
	TimeStart TimeType = "StartTime"
	TimeEnd   TimeType = "EndTime"
)

func (t TimeType) GetTaskTime(times *Times) time.Time {
	switch t {
	case TimeQueue:
		return times.queueTime
	case TimeStart:
		return times.startTime
	case TimeEnd:
		return times.endTime
	}
	return time.Time{}
}

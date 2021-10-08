// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package timerecord

import (
	"strconv"
	"time"

	"github.com/milvus-io/milvus/internal/log"
)

// TimeRecorder provides methods to record time duration
type TimeRecorder struct {
	header string
	start  time.Time
	last   time.Time
}

// NewTimeRecorder create a new TimeRecorder
func NewTimeRecorder(header string) *TimeRecorder {
	return &TimeRecorder{
		header: header,
		start:  time.Now(),
		last:   time.Now(),
	}
}

// RecordSpan return the duration from last record
func (tr *TimeRecorder) RecordSpan() time.Duration {
	curr := time.Now()
	span := curr.Sub(tr.last)
	tr.last = curr
	return span
}

// ElapseSpan return the duration from the beginning
func (tr *TimeRecorder) ElapseSpan() time.Duration {
	curr := time.Now()
	span := curr.Sub(tr.start)
	tr.last = curr
	return span
}

// Record calculates the time span from previous Record call
func (tr *TimeRecorder) Record(msg string) time.Duration {
	span := tr.RecordSpan()
	tr.printTimeRecord(msg, span)
	return span
}

// Elapse calculates the time span from the beginning of this TimeRecorder
func (tr *TimeRecorder) Elapse(msg string) time.Duration {
	span := tr.ElapseSpan()
	tr.printTimeRecord(msg, span)
	return span
}

func (tr *TimeRecorder) printTimeRecord(msg string, span time.Duration) {
	str := ""
	if len(tr.header) != 0 {
		str += tr.header + ": "
	}
	str += msg
	str += " ("
	str += strconv.Itoa(int(span.Milliseconds()))
	str += "ms)"
	log.Debug(str)
}

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

package datacoord

import "sync"

type queryViewLoadInfoNotification struct {
	collectionID int64
	segmentIDs   []int64
}

type queryViewLoadInfoNotificationRecorder struct {
	mu                   sync.Mutex
	segmentNotifications []queryViewLoadInfoNotification
}

func newQueryViewLoadInfoNotificationRecorder() *queryViewLoadInfoNotificationRecorder {
	return &queryViewLoadInfoNotificationRecorder{}
}

func (r *queryViewLoadInfoNotificationRecorder) NotifySegments(collectionID int64, segmentIDs ...int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segmentNotifications = append(r.segmentNotifications, queryViewLoadInfoNotification{
		collectionID: collectionID,
		segmentIDs:   append([]int64(nil), segmentIDs...),
	})
}

func (r *queryViewLoadInfoNotificationRecorder) segments() []queryViewLoadInfoNotification {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]queryViewLoadInfoNotification(nil), r.segmentNotifications...)
}

func (r *queryViewLoadInfoNotificationRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segmentNotifications = nil
}

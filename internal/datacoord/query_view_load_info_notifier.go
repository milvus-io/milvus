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

// QueryViewLoadInfoNotifier invalidates QueryNode segment load-info snapshots
// after the corresponding DataCoord metadata has been durably committed.
// Notifications are best-effort wakeups: QueryNode recovery always rebuilds
// state from the durable complete snapshot.
type QueryViewLoadInfoNotifier interface {
	NotifySegments(collectionID int64, segmentIDs ...int64)
}

type noopQueryViewLoadInfoNotifier struct{}

func (noopQueryViewLoadInfoNotifier) NotifySegments(int64, ...int64) {}

func (s *Server) SetQueryViewLoadInfoNotifier(notifier QueryViewLoadInfoNotifier) {
	if notifier == nil {
		notifier = noopQueryViewLoadInfoNotifier{}
	}
	s.queryViewLoadInfoNotifier = notifier
	if s.meta != nil {
		s.meta.queryViewLoadInfoNotifier = notifier
	}
}

func (m *meta) notifyQueryViewSegments(collectionID int64, segmentIDs ...int64) {
	if m != nil && m.queryViewLoadInfoNotifier != nil {
		m.queryViewLoadInfoNotifier.NotifySegments(collectionID, segmentIDs...)
	}
}

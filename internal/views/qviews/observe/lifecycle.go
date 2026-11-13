// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package observe

import (
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

var coordLifeStages = [...]*stage.Recorder{
	stage.New("coord", "view_load", "created_to_persisted"),
	stage.New("coord", "view_load", "persisted_to_all_ready"),
	stage.New("coord", "view_load", "all_ready_to_up_enqueue"),
	stage.New("coord", "view_load", "up_enqueue_to_confirmed"),
}

var viewLifeTotals = map[string]*stage.Recorder{
	componentCoord:         stage.New(componentCoord, "view_load", "total"),
	componentQueryNode:     stage.New(componentQueryNode, "view_load", "prepare"),
	componentStreamingNode: stage.New(componentStreamingNode, "view_load", "prepare"),
}

type viewLifecycle struct {
	phase int
	since time.Time
	total stage.Timer
}

// Lifecycle timestamps are independent of the existing state-age TopK. Progress
// reports and reconnects never reset them. The map shares the observer's existing
// mutex; completed intervals are recorded after releasing it.
func (o *MetricsObserver) observeLifecycle(event Event) {
	component := event.ComponentInfo()
	var view qviews.QueryViewKey
	begin, finish := false, false
	boundary := -1
	result := stage.Success
	switch e := event.(type) {
	case CoordViewCreatedEvent:
		view = e.View
		begin = true
	case QueryNodeAcquireSegmentsEvent:
		view = e.View
		begin = true
	case StreamingNodeAcquireResourceEvent:
		view = e.View
		begin = true
	case CoordPersistViewEvent:
		view = e.View
		if e.State == qviews.QueryViewStatePreparing {
			boundary = 0
		}
	case CoordSyncViewAcceptedEvent:
		view = e.View
		if e.State == qviews.QueryViewStateUp {
			boundary = 2
		}
	case CoordViewReportAppliedEvent:
		view = e.View
		if e.To == qviews.QueryViewStateReady {
			boundary = 1
		}
		if e.To == qviews.QueryViewStateUp {
			finish = true
			boundary = 3
		}
		if e.To == qviews.QueryViewStateUnrecoverable {
			finish = true
			result = stage.Error
		}
	case QueryNodeSegmentsReadyEvent:
		view = e.View
		finish = true
	case StreamingNodeResourceReadyEvent:
		view = e.View
		finish = true
	case StreamingNodeReportViewEvent:
		view = e.View
		if e.State == qviews.QueryViewStateUnrecoverable {
			finish = true
			result = stage.Error
		}
	case QueryNodeReleaseSegmentsEvent:
		view = e.View
		finish = true
		result = stage.Canceled
	case StreamingNodeReleaseResourceEvent:
		view = e.View
		finish = true
		result = stage.Canceled
	case CoordViewPreemptedEvent:
		view = e.View
		finish = true
		result = stage.Superseded
	case CoordViewReleaseRequestedEvent:
		view = e.View
		finish = true
		result = stage.Canceled
	case CoordViewQueryNodeLostAppliedEvent:
		view = e.View
		if e.To == qviews.QueryViewStateUnrecoverable {
			finish = true
			result = stage.Error
		}
	case QueryNodeSegmentUnrecoverableEvent:
		view = e.View
		finish = true
		result = stage.Error
	default:
		return
	}
	if !begin && !finish && boundary < 0 {
		return
	}
	key := metricViewKey{component: component, view: view}
	o.mu.Lock()
	now := o.now()
	life := o.lifecycles[key]
	if begin && life == nil {
		life = &viewLifecycle{since: now, total: viewLifeTotals[component].Begin()}
		o.lifecycles[key] = life
	}
	var recorder *stage.Recorder
	var duration time.Duration
	missing := false
	if life != nil && component == componentCoord && boundary == life.phase {
		recorder = coordLifeStages[life.phase]
		duration = now.Sub(life.since)
		life.phase++
		life.since = now
	}
	if finish && life != nil {
		// A callback may race SyncViews' return. Do not fabricate a timestamp or
		// retain the view forever waiting for a late enqueue observation.
		missing = result == stage.Success && component == componentCoord && life.phase != len(coordLifeStages)
		delete(o.lifecycles, key)
	}
	o.mu.Unlock()
	if recorder != nil {
		recorder.Observe(duration, result)
	}
	if finish && life != nil {
		if missing {
			metrics.QueryStageItems.WithLabelValues(component, "view_load", "coverage", "incomplete_order").Inc()
		} else if result == stage.Success {
			metrics.QueryStageItems.WithLabelValues(component, "view_load", "coverage", "complete").Inc()
		}
		life.total.EndResult(result)
	}
}

// ObserveLifecycleBoundary records a view-load boundary that no state event
// reports. Persist and sync acceptance happen for every flushed view, so they
// bypass the log observers and the existing state metrics.
func ObserveLifecycleBoundary(event Event) {
	defaultMetricsObserver.observeLifecycle(event)
}

// CancelView ends telemetry when an owner tears down a resident view without a
// state-machine release event (shutdown or WAL handoff).
func CancelView(component string, view qviews.QueryViewKey) {
	defaultMetricsObserver.cancelView(component, view)
}

func (o *MetricsObserver) cancelView(component string, view qviews.QueryViewKey) {
	key := metricViewKey{component: component, view: view}
	o.mu.Lock()
	life := o.lifecycles[key]
	delete(o.lifecycles, key)
	o.mu.Unlock()
	if life != nil {
		life.total.EndResult(stage.Canceled)
	}
}

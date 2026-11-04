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

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
)

// backgroundManifestIndexRecovery runs ReloadSegmentIndexesFromManifests in the
// background during startup.
type backgroundManifestIndexRecovery struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// startManifestIndexRecovery starts recovering m's segment indexes from
// segment manifests. The caller must call wait before it uses the indexes, or
// abort when it gives up on m.
func startManifestIndexRecovery(ctx context.Context, m *meta.Meta) *backgroundManifestIndexRecovery {
	ctx, cancel := context.WithCancel(ctx)
	r := &backgroundManifestIndexRecovery{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.err = m.ReloadSegmentIndexesFromManifests(ctx)
	}()
	return r
}

// wait returns the error of the recovery once it has finished.
func (r *backgroundManifestIndexRecovery) wait() error {
	<-r.done
	return r.err
}

// abort cancels the recovery if it is still running and waits for it to stop.
func (r *backgroundManifestIndexRecovery) abort() {
	r.cancel()
	<-r.done
}

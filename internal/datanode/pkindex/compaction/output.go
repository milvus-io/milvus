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

package compaction

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// errOutputIDsExhausted means the reserved ID range was too small for the
// outputs. Nothing re-plans with a bigger range today: the task fails and the
// vchannel waits for its cooldown.
var (
	errOutputDone       = errors.New("pkindex compaction output is finished or aborted")
	errOutputAborted    = errors.New("pkindex compaction output was aborted")
	errUploaderFinished = errors.New("pkindex compaction uploader is finished")
)

var errOutputIDsExhausted = errors.New("pkindex compaction ran out of output ids")

// splitOutput writes the merged entries to tables of about maxBytes on disk
// each (the estimated size: the pending data block is counted before
// compression and the filter block is not counted), with IDs from [nextID, endID)
// in order. onClose is called with each table as it is closed, before the
// next one opens. When onClose fails, the table is already closed and may be
// uploaded; the caller abandons the whole output and the GC collects the
// reserved IDs. After Abort, Finish fails; after Finish, Add fails.
type splitOutput struct {
	dir      string
	nextID   int64
	endID    int64
	maxBytes int64
	onClose  func(sst.Info) error

	w        *sst.Writer
	infos    []sst.Info
	finished bool
	aborted  bool
}

func (o *splitOutput) Add(key, value []byte) error {
	if o.finished || o.aborted {
		return errOutputDone
	}
	if o.w == nil {
		if o.nextID >= o.endID {
			return errOutputIDsExhausted
		}
		w, err := sst.NewWriter(o.dir, sst.ID(o.nextID))
		if err != nil {
			return err
		}
		o.w = w
		o.nextID++
	}
	if err := o.w.Add(key, value); err != nil {
		return err
	}
	if int64(o.w.EstimatedSize()) >= o.maxBytes {
		return o.closeCurrent()
	}
	return nil
}

func (o *splitOutput) closeCurrent() error {
	info, err := o.w.Close()
	o.w = nil
	if err != nil {
		return err
	}
	o.infos = append(o.infos, info)
	if o.onClose != nil {
		return o.onClose(info)
	}
	return nil
}

// Finish closes the open table, if any, and returns every table written.
func (o *splitOutput) Finish() ([]sst.Info, error) {
	if o.aborted {
		return nil, errOutputAborted
	}
	o.finished = true
	if o.w != nil {
		if err := o.closeCurrent(); err != nil {
			return nil, err
		}
	}
	return o.infos, nil
}

// Abort discards the open table, if any.
func (o *splitOutput) Abort() {
	o.aborted = true
	if o.w != nil {
		o.w.Abort()
		o.w = nil
	}
}

// uploader uploads closed tables while the merge writes the next one. The
// queue has room for one table, so the merge is at most one table ahead of
// the upload. The first failure is kept and later tables are skipped. Call
// finish exactly once on every path, success or error, or the goroutine
// leaks. It is not safe for concurrent calls of enqueue and finish.
type uploader struct {
	ctx      context.Context
	ch       chan sst.Info
	done     chan struct{}
	mu       sync.Mutex
	err      error
	finished bool
}

func startUploader(ctx context.Context, store Store, basePath, dir string) *uploader {
	u := &uploader{ctx: ctx, ch: make(chan sst.Info, 1), done: make(chan struct{})}
	go func() {
		defer close(u.done)
		for info := range u.ch {
			if u.failure() != nil {
				continue
			}
			path := filepath.Join(dir, sst.FileName(info.ID))
			if err := store.UploadFile(ctx, pkindex.SSTKey(basePath, int64(info.ID)), path); err != nil {
				u.mu.Lock()
				u.err = err
				u.mu.Unlock()
				continue
			}
			os.Remove(path) // uploaded: the local copy is not needed
		}
	}()
	return u
}

func (u *uploader) failure() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.err
}

// enqueue hands a closed table to the uploader. It returns the first upload
// failure, so that the merge stops early. It returns an error after finish,
// and the error of ctx when ctx ends while the queue is full.
func (u *uploader) enqueue(info sst.Info) error {
	u.mu.Lock()
	if u.finished {
		u.mu.Unlock()
		return errUploaderFinished
	}
	err := u.err
	u.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case u.ch <- info:
		return nil
	case <-u.ctx.Done():
		return u.ctx.Err()
	}
}

// finish waits for the queued uploads and returns the first failure. Call it
// exactly once on every path.
func (u *uploader) finish() error {
	u.mu.Lock()
	u.finished = true
	u.mu.Unlock()
	close(u.ch)
	<-u.done
	return u.failure()
}

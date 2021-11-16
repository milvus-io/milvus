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
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

const (
	// temp solution use const
	// maybe put garbage config into config files in the future
	defaultGcInterval       = 1 * time.Hour
	defaultMissingTolerance = 24 * time.Hour // 1day
	defaultDropTolerance    = 24 * time.Hour // 1day
)

// GcOption garbage collection options
type GcOption struct {
	cli              *minio.Client // OSS client
	enabled          bool          // enable switch
	checkInterval    time.Duration // each interval
	missingTolerance time.Duration // key missing in meta tolerace time
	dropTolerance    time.Duration // dropped segment related key tolerance time
	bucketName       string
	rootPath         string
}

// garbageCollector handles garbage files in object storage
// which could be dropped collection remnant or data node failure traces
type garbageCollector struct {
	option GcOption
	meta   *meta

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
	closeCh   chan struct{}
}

// newGarbageCollector create garbage collector with meta and option
func newGarbageCollector(meta *meta, opt GcOption) *garbageCollector {
	return &garbageCollector{
		meta:    meta,
		option:  opt,
		closeCh: make(chan struct{}),
	}
}

// start a goroutine and perform gc check every `checkInterval`
func (gc *garbageCollector) start() {
	if gc.option.enabled {
		if gc.option.cli == nil {
			log.Warn("datacoord gc enabled, but SSO client is not provided")
			return
		}
		gc.startOnce.Do(func() {
			gc.wg.Add(1)
			go gc.work()
		})
	}
}

// work contains actual looping check logic
func (gc *garbageCollector) work() {
	defer gc.wg.Done()
	ticker := time.Tick(gc.option.checkInterval)
	for {
		select {
		case <-ticker:
			gc.scan()
		case <-gc.closeCh:
			log.Warn("garbage collector quit")
			return
		}
	}
}

func (gc *garbageCollector) close() {
	gc.stopOnce.Do(func() {
		close(gc.closeCh)
		gc.wg.Wait()
	})
}

// scan load meta file info and compares OSS keys
// if drop found or missing found, performs gc cleanup
func (gc *garbageCollector) scan() {
	var v, d, m, e int
	valid, dropped, droppedAt := gc.meta.ListSegmentFiles()
	vm := make(map[string]struct{})
	dm := make(map[string]uint64)
	for _, k := range valid {
		vm[k] = struct{}{}
	}
	for i, k := range dropped {
		dm[k] = droppedAt[i]
	}

	for info := range gc.option.cli.ListObjects(context.TODO(), gc.option.bucketName, minio.ListObjectsOptions{
		Prefix:    gc.option.rootPath,
		Recursive: true,
	}) {
		log.Warn(info.Key)
		_, has := vm[info.Key]
		if has {
			v++
			continue
		}
		// dropped
		droppedTs, has := dm[info.Key]
		if has {
			d++
			droppedTime := time.Unix(0, int64(droppedTs))
			// check file last modified time exceeds tolerance duration
			if time.Since(droppedTime) > gc.option.dropTolerance {
				e++
				// ignore error since it could be cleaned up next time
				_ = gc.option.cli.RemoveObject(context.TODO(), gc.option.bucketName, info.Key, minio.RemoveObjectOptions{})
			}
			continue
		}
		m++
		// not found in meta, check last modified time exceeds tolerance duration
		if time.Since(info.LastModified) > gc.option.missingTolerance {
			e++
			// ignore error since it could be cleaned up next time
			_ = gc.option.cli.RemoveObject(context.TODO(), gc.option.bucketName, info.Key, minio.RemoveObjectOptions{})
		}
	}
	log.Warn("scan result", zap.Int("valid", v), zap.Int("dropped", d), zap.Int("missing", m), zap.Int("removed", e))
}

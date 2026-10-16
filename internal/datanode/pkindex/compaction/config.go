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
	"path/filepath"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Config is what the executor needs from the configuration.
type Config struct {
	// LocalRoot is the directory of the task directories. The executor
	// deletes it at start.
	LocalRoot string
	// TerminalTaskTTL is how long a finished or failed task waits for a
	// query before the executor deletes it.
	TerminalTaskTTL time.Duration
	// TaskSlot is what one running task takes from the node slots.
	TaskSlot int64
	// Parallelism is the number of tasks that run at once.
	Parallelism int
	// DiskMargin is the free disk a task needs on top of twice its inputs.
	DiskMargin int64
}

// ConfigFromParams reads the configuration from the param table.
func ConfigFromParams() Config {
	p := paramtable.Get()
	return Config{
		LocalRoot:       filepath.Join(p.LocalStorageCfg.Path.GetValue(), "pkindex_compaction"),
		TerminalTaskTTL: p.DataNodeCfg.PKIndexCompactionTerminalTaskTTL.GetAsDuration(time.Second),
		TaskSlot:        p.DataCoordCfg.PKIndexCompactionSlotUsage.GetAsInt64(),
		Parallelism:     p.DataNodeCfg.PKIndexCompactionParallelism.GetAsInt(),
		DiskMargin:      p.DataNodeCfg.PKIndexCompactionDiskMargin.GetAsInt64(),
	}
}

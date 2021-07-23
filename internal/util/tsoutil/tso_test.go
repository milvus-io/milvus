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

package tsoutil

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

func TestParseHybridTs(t *testing.T) {
	var ts uint64 = 426152581543231492
	physical, logical := ParseHybridTs(ts)
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	log.Debug("TestParseHybridTs",
		zap.Uint64("physical", physical),
		zap.Uint64("logical", logical),
		zap.Any("physical time", physicalTime))
}

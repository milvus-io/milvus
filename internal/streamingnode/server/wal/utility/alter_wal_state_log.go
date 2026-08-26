// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utility

import (
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// AlterWALStateLogFields returns typed log fields without Configs payload.
//
// AlterWALState.Configs is the target broker's own configuration, taken
// verbatim from the /management/wal/alter request body, so it is where
// sasl.password and ssl.key.pem style material arrives. Unlike a config value
// read through config.Manager it carries no declared sensitivity metadata, and
// unlike the request itself this copy is persisted into the WAL checkpoint —
// so logging it prints the credential again on every restart, forever. The
// map keys are caller-controlled too, so the log retains only their count.
func AlterWALStateLogFields(state *streamingpb.AlterWALState) []mlog.Field {
	if state == nil {
		return nil
	}
	return []mlog.Field{
		mlog.String("alterWALStage", state.GetStage().String()),
		mlog.String("targetWALName", state.GetTargetWalName().String()),
		mlog.Int("alterWALConfigCount", len(state.GetConfigs())),
	}
}

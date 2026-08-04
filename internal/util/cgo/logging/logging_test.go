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

package logging

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestLogging(t *testing.T) {
	require.Equal(t, mlog.InfoLevel, mapGlogSeverity(0))
	require.Equal(t, mlog.WarnLevel, mapGlogSeverity(1))
	require.Equal(t, mlog.ErrorLevel, mapGlogSeverity(2))
	require.Equal(t, mlog.ErrorLevel, mapGlogSeverity(3))
	require.Equal(t, mlog.InfoLevel, mapGlogSeverity(4))
}

func TestMapTantivySeverity(t *testing.T) {
	require.Equal(t, mlog.DebugLevel, mapTantivySeverity(tantivyTrace))
	require.Equal(t, mlog.DebugLevel, mapTantivySeverity(tantivyDebug))
	require.Equal(t, mlog.InfoLevel, mapTantivySeverity(tantivyInfo))
	require.Equal(t, mlog.WarnLevel, mapTantivySeverity(tantivyWarn))
	require.Equal(t, mlog.ErrorLevel, mapTantivySeverity(tantivyError))
	require.Equal(t, mlog.InfoLevel, mapTantivySeverity(99))
}

func TestLogTantivyRecord(t *testing.T) {
	oldLogger := mlog.L()
	core, observed := observer.New(zapcore.DebugLevel)
	mlog.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(func() { mlog.ReplaceGlobals(oldLogger, nil) })

	logTantivyRecord(
		tantivyDebug,
		"tantivy::indexer::segment_updater",
		"tantivy/src/indexer/segment_updater.rs",
		42,
		"merge completed",
	)

	entries := observed.AllUntimed()
	require.Len(t, entries, 1)
	require.Equal(t, zapcore.DebugLevel, entries[0].Level)
	require.Equal(t, "Tantivy/tantivy::indexer::segment_updater", entries[0].LoggerName)
	require.Equal(t, "merge completed", entries[0].Message)
	require.True(t, entries[0].Caller.Defined)
	require.Equal(t, "tantivy/src/indexer/segment_updater.rs", entries[0].Caller.File)
	require.Equal(t, 42, entries[0].Caller.Line)
}

func TestLogTantivyRecordRespectsCoreLevel(t *testing.T) {
	oldLogger := mlog.L()
	core, observed := observer.New(zapcore.InfoLevel)
	mlog.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(func() { mlog.ReplaceGlobals(oldLogger, nil) })

	logTantivyRecord(tantivyDebug, "tantivy::background", "", 0, "hidden")
	require.Equal(t, 0, observed.Len())
}

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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

type capturedLogEntry struct {
	Level   string `json:"level"`
	Name    string `json:"name"`
	Caller  string `json:"caller"`
	Message string `json:"message"`
}

func captureMlogEntries(t *testing.T, level string) func() []capturedLogEntry {
	t.Helper()

	logDir := t.TempDir()
	logFile := filepath.Join(logDir, "tantivy.log")
	logger, props, err := mlog.InitLogger(&mlog.Config{
		Level:             level,
		Format:            "json",
		DisableTimestamp:  true,
		DisableStacktrace: true,
		File: mlog.FileLogConfig{
			RootPath: logDir,
			Filename: "tantivy.log",
		},
	})
	require.NoError(t, err)
	mlog.ReplaceGlobals(logger, props)
	t.Cleanup(func() {
		require.NoError(t, logger.Sync())
		restoreLogger, restoreProps, err := mlog.InitTestLogger(t, &mlog.Config{
			Level:             "info",
			DisableTimestamp:  true,
			DisableCaller:     true,
			DisableStacktrace: true,
		})
		require.NoError(t, err)
		mlog.ReplaceGlobals(restoreLogger, restoreProps)
	})

	return func() []capturedLogEntry {
		t.Helper()
		require.NoError(t, logger.Sync())
		content, err := os.ReadFile(logFile)
		require.NoError(t, err)

		trimmed := strings.TrimSpace(string(content))
		if trimmed == "" {
			return nil
		}

		lines := strings.Split(trimmed, "\n")
		entries := make([]capturedLogEntry, 0, len(lines))
		for _, line := range lines {
			var entry capturedLogEntry
			require.NoError(t, json.Unmarshal([]byte(line), &entry))
			entries = append(entries, entry)
		}
		return entries
	}
}

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
	readEntries := captureMlogEntries(t, "debug")

	logTantivyRecord(
		tantivyDebug,
		"tantivy::indexer::segment_updater",
		"tantivy/src/indexer/segment_updater.rs",
		42,
		"merge completed",
	)

	entries := readEntries()
	require.Len(t, entries, 1)
	require.Equal(t, "DEBUG", entries[0].Level)
	require.Equal(t, "Tantivy/tantivy::indexer::segment_updater", entries[0].Name)
	require.Equal(t, "merge completed", entries[0].Message)
	require.True(t, strings.HasSuffix(entries[0].Caller, "indexer/segment_updater.rs:42"))
}

func TestLogTantivyRecordRespectsCoreLevel(t *testing.T) {
	readEntries := captureMlogEntries(t, "info")

	logTantivyRecord(tantivyDebug, "tantivy::background", "", 0, "hidden")
	logTantivyRecord(tantivyInfo, "tantivy::background", "", 0, "visible")

	entries := readEntries()
	require.Len(t, entries, 1)
	require.Equal(t, "visible", entries[0].Message)
}

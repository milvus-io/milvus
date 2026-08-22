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

//go:build test
// +build test

package logging

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTantivyLoggingBridge(t *testing.T) {
	readEntries := captureMlogEntries(t, "info")

	InitGoogleLoggingWithZapSink()
	require.False(t, tantivyIndexExist("/nonexistent/path/to/tantivy-index"))
	require.True(t, tantivyTestLogFromBackgroundThread())

	var matchingEntries []capturedLogEntry
	var backgroundEntries []capturedLogEntry
	for _, entry := range readEntries() {
		if strings.Contains(entry.Message, "failed to open directory") {
			matchingEntries = append(matchingEntries, entry)
		}
		if strings.HasPrefix(entry.Message, "bridge ") {
			backgroundEntries = append(backgroundEntries, entry)
		}
	}
	require.Len(t, matchingEntries, 1)
	require.Equal(t, "INFO", matchingEntries[0].Level)
	require.Equal(t, "Tantivy/tantivy_binding::util", matchingEntries[0].Name)
	require.Regexp(t, `src/util\.rs:\d+$`, matchingEntries[0].Caller)

	require.Len(t, backgroundEntries, 3)
	require.Equal(t, []string{"bridge info", "bridge warn", "bridge error"}, []string{
		backgroundEntries[0].Message,
		backgroundEntries[1].Message,
		backgroundEntries[2].Message,
	})
	require.Equal(t, []string{"INFO", "WARN", "ERROR"}, []string{
		backgroundEntries[0].Level,
		backgroundEntries[1].Level,
		backgroundEntries[2].Level,
	})
	for _, entry := range backgroundEntries {
		require.Equal(t, "Tantivy/tantivy::background", entry.Name)
		require.Regexp(t, `src/log_c\.rs:\d+$`, entry.Caller)
	}
}

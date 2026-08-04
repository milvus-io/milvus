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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestTantivyLoggingBridge(t *testing.T) {
	oldLogger := mlog.L()
	core, observed := observer.New(zapcore.InfoLevel)
	mlog.ReplaceGlobals(zap.New(core), nil)
	t.Cleanup(func() { mlog.ReplaceGlobals(oldLogger, nil) })

	InitGoogleLoggingWithZapSink()
	require.False(t, tantivyIndexExist("/nonexistent/path/to/tantivy-index"))

	entries := observed.FilterMessageSnippet("failed to open directory").AllUntimed()
	require.Len(t, entries, 1)
	require.Equal(t, zapcore.InfoLevel, entries[0].Level)
	require.Equal(t, "Tantivy/tantivy_binding::util", entries[0].LoggerName)
	require.True(t, entries[0].Caller.Defined)
	require.True(t, strings.HasSuffix(entries[0].Caller.File, "src/util.rs"))
	require.Positive(t, entries[0].Caller.Line)
}

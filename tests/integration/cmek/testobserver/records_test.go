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

package testobserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func canonicalRecords() []Record {
	registration := Record{Token: "run", NodeID: 7, PID: 11, Sequence: 1, Event: "registered", CollectionID: 19, Channel: "channel", Success: true}
	commit := Record{
		Token: "run", NodeID: 7, PID: 11, Sequence: 2, Event: "commit", TaskID: 1, TaskType: "SyncTask", CollectionID: 19, PartitionID: 23,
		SegmentID: 29, Channel: "channel", StorageVersion: 3, Rows: 2, FieldRows: map[int64]int64{0: 2, 1: 2}, Manifest: `{"base_path":"root/29","ver":1}`, Success: true,
	}
	completed := commit
	completed.Sequence, completed.Event = 3, "task"
	return []Record{registration, commit, completed}
}

func TestRecorderAndCollectorPreserveConcurrentRecords(t *testing.T) {
	directory := t.TempDir()
	recorder, err := NewRecorder(directory, "run", 7, 11)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errors := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- recorder.Append(Record{Event: "registered", Channel: "channel", CollectionID: 19})
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	records, err := ReadRecords(directory, "run", map[int64]int{7: 11})
	require.NoError(t, err)
	require.Len(t, records, 20)
	for i, record := range records {
		require.EqualValues(t, i+1, record.Sequence)
	}
}

func TestCollectorRejectsWrongOwnerTokenAndLostRecords(t *testing.T) {
	for _, test := range []struct {
		name     string
		mutate   func([]Record) []Record
		truncate bool
	}{
		{"token", func(r []Record) []Record { r[1].Token = "wrong"; return r }, false},
		{"owner", func(r []Record) []Record { r[1].PID = 99; return r }, false},
		{"missing record", func(r []Record) []Record { return append(r[:1], r[2:]...) }, false},
		{"duplicate sequence", func(r []Record) []Record { r[2].Sequence = 2; return r }, false},
		{"truncated record", func(r []Record) []Record { return r }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			var contents []byte
			for _, record := range test.mutate(canonicalRecords()) {
				encoded, err := json.Marshal(record)
				require.NoError(t, err)
				contents = append(contents, append(encoded, '\n')...)
			}
			if test.truncate {
				contents = contents[:len(contents)-3]
			}
			require.NoError(t, os.WriteFile(filepath.Join(directory, "7-11.jsonl"), contents, 0o600))
			_, err := ReadRecords(directory, "run", map[int64]int{7: 11})
			require.Error(t, err)
		})
	}
}

func TestCanonicalFlushRequiresWriterCommitAndTaskSuccess(t *testing.T) {
	records, complete, err := CanonicalFlushes(canonicalRecords(), 19, map[string]bool{"channel": true}, 2)
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, records, 1)
	for _, test := range []struct {
		name   string
		mutate func([]Record) []Record
	}{
		{"writer only", func(r []Record) []Record { return r[:2] }},
		{"lost tail", func(r []Record) []Record { return r[:1] }},
		{"commit rejected but task returned nil", func(r []Record) []Record { r[1].Success = false; return r }},
		{"failed task", func(r []Record) []Record { r[2].Success = false; return r }},
		{"zero row completion", func(r []Record) []Record { r[1].Rows = 0; r[2].Rows = 0; return r }},
		{"ack retry without writer output", func(r []Record) []Record { r[1].FieldRows = nil; r[2].FieldRows = nil; return r }},
		{"missing original manifest", func(r []Record) []Record { r[1].Manifest = ""; r[2].Manifest = ""; return r }},
		{"different committed manifest", func(r []Record) []Record { r[1].Manifest = "different"; return r }},
		{"different task owner", func(r []Record) []Record { r[2].NodeID = 8; return r }},
		{"unexpected source", func(r []Record) []Record { r[2].TaskType = "GrowingSourceSyncTask"; return r }},
		{"incomplete insert binlog", func(r []Record) []Record {
			r[1].FieldRows = map[int64]int64{0: 1}
			r[2].FieldRows = map[int64]int64{0: 1}
			return r
		}},
		{"duplicate completion", func(r []Record) []Record { return append(r, r[2]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, complete, _ := CanonicalFlushes(test.mutate(canonicalRecords()), 19, map[string]bool{"channel": true}, 2)
			require.False(t, complete)
		})
	}
}

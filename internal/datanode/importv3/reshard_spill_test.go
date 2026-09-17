// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"io"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func spillTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "note", DataType: schemapb.DataType_VarChar},
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		},
	}
}

// spillTestBatch builds one routed-fragment-shaped batch of n rows starting
// at pkStart, mimicking what hash routing hands to the spill log.
func spillTestBatch(pkStart int64, n int) (*storage.InsertData, int64) {
	ids := make([]int64, n)
	rowIDs := make([]int64, n)
	notes := make([]string, n)
	for i := range ids {
		ids[i] = pkStart + int64(i)
		rowIDs[i] = pkStart + int64(i)
		notes[i] = "note"
	}
	data := &storage.InsertData{Data: map[int64]storage.FieldData{
		100:               &storage.Int64FieldData{Data: ids},
		101:               &storage.StringFieldData{Data: notes},
		common.RowIDField: &storage.Int64FieldData{Data: rowIDs},
	}}
	return data, int64(data.GetMemorySize())
}

func readRange(t *testing.T, log *SpillLog, r SpillRange) (records, rows int, pks []int64) {
	t.Helper()
	reader, err := log.RangeReader(r)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	for {
		record, err := reader.Next()
		if err == io.EOF {
			return records, rows, pks
		}
		require.NoError(t, err)
		records++
		rows += record.Len()
		col := record.Column(100).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			pks = append(pks, col.Value(i))
		}
	}
}

// TestSpillLogAppendAndRangeRead pins the core contract: an appended bucket
// tail comes back through its range with rows, values and metrics intact,
// small fragments are coalesced into few records, and several ranges of the
// same shard share one file at disjoint offsets while ranges of different
// buckets land in different shard files.
func TestSpillLogAppendAndRangeRead(t *testing.T) {
	dir := t.TempDir()
	log, err := NewSpillLog(dir, spillTestSchema(), 2)
	require.NoError(t, err)
	defer func() { _ = log.Close() }()

	var batches []SpillBatch
	for i := 0; i < 10; i++ {
		data, size := spillTestBatch(int64(i*4), 4)
		batches = append(batches, SpillBatch{Data: data, Bytes: size})
	}
	// Nil and empty batches are skipped without breaking the range.
	empty, _ := spillTestBatch(0, 0)
	batches = append(batches, SpillBatch{Data: nil}, SpillBatch{Data: empty})

	r1, err := log.Append(0, batches)
	require.NoError(t, err)
	require.Equal(t, int64(40), r1.Rows)
	require.Equal(t, int32(0), r1.Stream)
	require.Greater(t, r1.Begin, int64(0), "the stream header and kick record precede every range")
	require.Greater(t, r1.End, r1.Begin)
	var wantLogical int64
	for _, b := range batches[:10] {
		wantLogical += b.Bytes
	}
	require.Equal(t, wantLogical, r1.Logical)

	records, rows, pks := readRange(t, log, r1)
	require.Equal(t, 40, rows)
	require.Equal(t, 1, records, "40 tiny fragments must coalesce into one record, not one record each")
	for i, pk := range pks {
		require.Equal(t, int64(i), pk)
	}

	// A second append to the same shard lands after the first range in the
	// same file; a different bucket maps to its own shard file.
	data, size := spillTestBatch(100, 5)
	r2, err := log.Append(0, []SpillBatch{{Data: data, Bytes: size}})
	require.NoError(t, err)
	require.Equal(t, int32(0), r2.Stream)
	require.Equal(t, r1.End, r2.Begin)
	require.Equal(t, int64(5), r2.Rows)
	r3, err := log.Append(1, []SpillBatch{{Data: data, Bytes: size}})
	require.NoError(t, err)
	require.Equal(t, int32(1), r3.Stream)

	_, rows2, pks2 := readRange(t, log, r2)
	require.Equal(t, 5, rows2)
	require.Equal(t, []int64{100, 101, 102, 103, 104}, pks2)
	_, rows3, _ := readRange(t, log, r3)
	require.Equal(t, 5, rows3)
	// The first range is still readable after later appends.
	_, rows1, _ := readRange(t, log, r1)
	require.Equal(t, 40, rows1)
	require.Equal(t, 2, log.Files())
}

// TestSpillLogReleasesAndRecreatesShardFiles pins the shard lifecycle: a
// shard shared by several buckets survives until every one of their ranges
// is released, a fully released shard file is removed from disk right away,
// and a later append recreates the dormant shard with a readable range.
func TestSpillLogReleasesAndRecreatesShardFiles(t *testing.T) {
	dir := t.TempDir()
	log, err := NewSpillLog(dir, spillTestSchema(), 2)
	require.NoError(t, err)
	defer func() { _ = log.Close() }()

	data, size := spillTestBatch(1, 100)
	// Buckets 0 and 2 share shard 0; bucket 1 owns shard 1.
	r0, err := log.Append(0, []SpillBatch{{Data: data, Bytes: size}})
	require.NoError(t, err)
	r2, err := log.Append(2, []SpillBatch{{Data: data, Bytes: size}})
	require.NoError(t, err)
	r1, err := log.Append(1, []SpillBatch{{Data: data, Bytes: size}})
	require.NoError(t, err)
	require.Equal(t, r0.Stream, r2.Stream)
	file0 := path.Join(dir, "0.arrow")
	file1 := path.Join(dir, "1.arrow")

	// Releasing one bucket of a shared shard keeps the file for the others.
	log.Release([]SpillRange{r0})
	_, err = os.Stat(file0)
	require.NoError(t, err, "a shard with live ranges of another bucket must survive")
	_, rows2, _ := readRange(t, log, r2)
	require.Equal(t, 100, rows2)

	// Releasing the last range removes the shard file immediately.
	log.Release([]SpillRange{r2})
	_, err = os.Stat(file0)
	require.True(t, os.IsNotExist(err), "a fully released shard must be removed, got: %v", err)

	// A later append recreates the dormant shard and reads back.
	r0b, err := log.Append(0, []SpillBatch{{Data: data, Bytes: size}})
	require.NoError(t, err)
	require.Equal(t, int32(0), r0b.Stream)
	_, rows0b, _ := readRange(t, log, r0b)
	require.Equal(t, 100, rows0b)

	// The untouched shard is unaffected by the other's lifecycle.
	_, rows1, _ := readRange(t, log, r1)
	require.Equal(t, 100, rows1)
	_, err = os.Stat(file1)
	require.NoError(t, err)
}

// TestSpillLogCloseLeavesValidStream pins that Close terminates the open
// stream with its EOS marker, leaving a materialized shard a valid
// standalone IPC stream readable without the range index.
func TestSpillLogCloseLeavesValidStream(t *testing.T) {
	dir := t.TempDir()
	log, err := NewSpillLog(dir, spillTestSchema(), 1)
	require.NoError(t, err)
	data, size := spillTestBatch(1, 10)
	_, err = log.Append(0, []SpillBatch{{Data: data, Bytes: size}})
	require.NoError(t, err)
	require.NoError(t, log.Close())

	file, err := os.Open(path.Join(dir, "0.arrow"))
	require.NoError(t, err)
	defer file.Close()
	reader, err := ipc.NewReader(file)
	require.NoError(t, err)
	defer reader.Release()
	var rows int
	for reader.Next() {
		rows += int(reader.Record().NumRows())
	}
	require.NoError(t, reader.Err())
	require.Equal(t, 10, rows)
}

// TestSpillLogCloseIsIdempotent pins that Close tolerates a log that never
// appended and repeated Close calls, matching the run-level defer.
func TestSpillLogCloseIsIdempotent(t *testing.T) {
	log, err := NewSpillLog(t.TempDir(), spillTestSchema(), 4)
	require.NoError(t, err)
	require.Equal(t, 4, log.Streams())
	require.NoError(t, log.Close())
	require.NoError(t, log.Close())
	require.Equal(t, 0, log.Files())
}

// TestSpillLogServesConcurrentAppendsAndReleases pins the locking contract the
// detached fragment writes rely on: the routing goroutine keeps appending
// bucket tails while several write goroutines replay and release their ranges
// on the same shards, including the retire-and-recreate path of a shard whose
// ranges are all released. Every spilled row must come back exactly once. Run
// under -race, which is how CI executes it.
func TestSpillLogServesConcurrentAppendsAndReleases(t *testing.T) {
	dir := t.TempDir()
	log, err := NewSpillLog(dir, spillTestSchema(), 4)
	require.NoError(t, err)
	defer func() { _ = log.Close() }()

	const (
		buckets = 8
		rounds  = 16
		writers = 4
		rows    = 4
	)
	type replay struct {
		rows int
		pks  []int64
		err  error
	}
	// An unbuffered handoff, so every append really interleaves with the
	// replays and releases of the ranges appended before it.
	ranges := make(chan SpillRange)
	replays := make(chan replay, buckets*rounds)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range ranges {
				got := replay{}
				reader, err := log.RangeReader(r)
				if err != nil {
					got.err = err
					replays <- got
					continue
				}
				for {
					record, err := reader.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						got.err = err
						break
					}
					got.rows += record.Len()
					col := record.Column(100).(*array.Int64)
					for i := 0; i < col.Len(); i++ {
						got.pks = append(got.pks, col.Value(i))
					}
				}
				_ = reader.Close()
				log.Release([]SpillRange{r})
				replays <- got
			}
		}()
	}

	var pk int64
	for round := 0; round < rounds; round++ {
		for bucket := 0; bucket < buckets; bucket++ {
			data, size := spillTestBatch(pk, rows)
			spillRange, err := log.Append(int64(bucket), []SpillBatch{{Data: data, Bytes: size}})
			require.NoError(t, err)
			require.Equal(t, int64(rows), spillRange.Rows)
			ranges <- spillRange
			pk += rows
		}
	}
	close(ranges)
	wg.Wait()
	close(replays)

	total := 0
	seen := make(map[int64]struct{}, int(pk))
	for got := range replays {
		require.NoError(t, got.err)
		total += got.rows
		for _, p := range got.pks {
			_, dup := seen[p]
			require.False(t, dup, "a spilled row must come back exactly once")
			seen[p] = struct{}{}
		}
	}
	require.Equal(t, buckets*rounds*rows, total)
	require.Len(t, seen, int(pk))
	// Every range came back and was released, so every shard retired: the spill
	// directory is empty again even though the concurrent releases drove the
	// retire-and-recreate path over and over.
	require.Positive(t, log.Files(), "the run must have materialized shard files")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

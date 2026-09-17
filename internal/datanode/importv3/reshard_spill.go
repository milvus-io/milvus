// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"bytes"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
	"syscall"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// spillRecordTargetBytes is the coalescing target of one arrow record inside
// a spilled range. Routing shreds a source batch into per-bucket fragments of
// a few rows each; replaying them one record per fragment makes storage.Sort
// retain millions of tiny records. Coalescing to quarter-MiB records keeps
// the replay record count proportional to bytes, not to fragments.
const spillRecordTargetBytes int64 = 256 << 10

// ipcEOSBytes is the size of the arrow IPC end-of-stream marker
// (continuation token plus a zero metadata length), used to trim the captured
// schema message out of a probe stream.
const ipcEOSBytes = 8

// SpillBatch is one routed fragment handed to SpillLog.Append. Bytes is the
// fragment's decoded logical size (GetMemorySize), the metric that drives
// record coalescing and travels inside the returned SpillRange.
type SpillBatch struct {
	Data  *storage.InsertData
	Bytes int64
}

// SpillRange locates one spilled bucket tail: a run of record-batch messages
// at [Begin, End) inside the single Arrow IPC stream of the fixed shard
// Stream. Buckets map to shards once and for all, so every range of one
// bucket lives in exactly one file and reading a bucket back never touches
// the other spill files. Each stream carries one schema message at its head,
// so reading a range is a matter of prepending the run's once-captured
// schema message bytes to the section -- the Spark-shuffle-style
// (offset, length) index over a shared data file. The temporary schema never
// contains dictionary fields, so no dictionary messages can live between the
// header and a range.
type SpillRange struct {
	Stream  int32
	Begin   int64
	End     int64
	Logical int64 // decoded bytes; feeds sort-group packing and the fragment descriptor
	Rows    int64
}

// spillStream is one fixed shard file. live counts the appended ranges that
// have not been released; once it hits zero the file is removed right away
// and the stream falls dormant -- a later append to one of its buckets
// recreates it. The append fd, stream writer and offset counter are nil
// while dormant.
type spillStream struct {
	path    string
	live    int
	file    *os.File
	iw      *ipc.Writer
	counted *countingWriter
}

// SpillLog is one reshard run's append-only local spill container. The run's
// buckets map by a fixed hash onto at most streamCount shard files
// (min(buckets, dataNode.import.reshardSpillMaxStreams), chosen by the
// caller), each holding ONE long-lived Arrow IPC stream -- the schema
// message is written once per file creation, not per spill event. The routing
// goroutine appends whole bucket tails as indexed ranges while the detached
// fragment writes replay their ranges and release them, so every entry point
// takes the log's mutex; the write offset stays a plain counter because that
// mutex serializes appends. Readers cache one fd per shard and address ranges
// through SectionReader with the run's captured schema message prepended: fd
// count, file count and per-bucket read fan-out are all bounded by streamCount
// no matter how many buckets exist, and two writes reading one shard do not
// interfere because a SectionReader only preads its own span. A range is
// released by the write that consumed it and a shard is removed only once none
// of its ranges are live, so no reader ever sees a removed file. The log is not
// durable state: a run restart re-executes from the sources and the node-local
// spill root is wiped on startup, so writes are never fsynced.
type SpillLog struct {
	mu        sync.Mutex
	dir       string
	schema    *schemapb.CollectionSchema
	arrow     *arrow.Schema
	schemaMsg []byte // the stream header bytes, captured once and prepended to every range read
	field2Col map[storage.FieldID]int
	streams   []*spillStream
	created   int // stream files materialized so far, for the run summary
	readFds   map[int32]*os.File
}

// NewSpillLog prepares a log appending into dir, which must exist.
// streamCount is the shard count the caller derived from the run's bucket
// count, clamped to at least one. No file is created until the first append
// into a stream, so a run that never spills leaves the directory empty. The
// schema message bytes every range read prepends are captured here once:
// closing a fresh IPC writer emits exactly [schema message][EOS].
func NewSpillLog(dir string, schema *schemapb.CollectionSchema, streamCount int) (*SpillLog, error) {
	arrowSchema, err := storage.ConvertToArrowSchema(schema, false)
	if err != nil {
		return nil, err
	}
	var probe bytes.Buffer
	probeWriter := ipc.NewWriter(&probe, ipc.WithSchema(arrowSchema))
	if err := probeWriter.Close(); err != nil {
		return nil, merr.Wrapf(err, "capture reshard spill schema message")
	}
	field2Col := make(map[storage.FieldID]int)
	for i, field := range typeutil.GetAllFieldSchemas(schema) {
		field2Col[field.GetFieldID()] = i
	}
	streams := make([]*spillStream, max(streamCount, 1))
	for i := range streams {
		streams[i] = &spillStream{path: path.Join(dir, strconv.Itoa(i)+".arrow")}
	}
	return &SpillLog{
		dir: dir, schema: schema, arrow: arrowSchema,
		schemaMsg: probe.Bytes()[:probe.Len()-ipcEOSBytes],
		field2Col: field2Col,
		streams:   streams,
		readFds:   make(map[int32]*os.File),
	}, nil
}

// Streams reports the number of fixed shards, for the run summary.
func (l *SpillLog) Streams() int {
	return len(l.streams)
}

// Files reports how many stream files have been materialized so far, for the
// run summary.
func (l *SpillLog) Files() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.created
}

// countingWriter tracks the exact file offset of everything the IPC writer
// emits, so a range's [begin, end) needs no post-hoc file stat.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// spillIOError keeps the run's resource-exhaustion contract: a full local
// disk is a retryable insufficient-resource system error, not a data bug.
func spillIOError(err error, what string) error {
	if errors.Is(err, syscall.ENOSPC) {
		return merr.Wrap(merr.ErrServiceResourceInsufficient, what+": "+err.Error())
	}
	return merr.Wrapf(err, "%s", what)
}

// ensureStream materializes a dormant stream's file and starts its single
// IPC stream. The ipc writer defers its schema message to the first record;
// the stream is kicked with one empty record so every range's begin is
// simply the current write offset, with no first-range special case. The
// kick costs a few hundred bytes and sits before all ranges, so no reader
// ever sees it.
func (l *SpillLog) ensureStream(s *spillStream) error {
	if s.iw != nil {
		return nil
	}
	file, err := os.Create(s.path)
	if err != nil {
		return spillIOError(err, "create reshard spill file "+s.path)
	}
	counted := &countingWriter{w: file}
	iw := ipc.NewWriter(counted, ipc.WithSchema(l.arrow))
	builder := array.NewRecordBuilder(memory.DefaultAllocator, l.arrow)
	kick := builder.NewRecord()
	err = iw.Write(kick)
	kick.Release()
	builder.Release()
	if err != nil {
		_ = file.Close()
		return spillIOError(err, "start reshard spill file "+s.path)
	}
	s.file, s.iw, s.counted = file, iw, counted
	l.created++
	return nil
}

// removeStream retires a stream whose ranges are all released: the cached
// read fd and the append fd are closed and the file is removed, reclaiming
// its disk without waiting for the run to end. The ipc writer is dropped
// without an EOS -- the file is going away. The stream falls dormant and is
// recreated by a later append.
func (l *SpillLog) removeStream(seq int32) {
	if fd, ok := l.readFds[seq]; ok {
		_ = fd.Close()
		delete(l.readFds, seq)
	}
	s := l.streams[seq]
	s.iw = nil
	s.counted = nil
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
	_ = os.Remove(s.path)
}

// Append serializes one bucket tail into the bucket's fixed stream and
// returns the range indexing it. The bucket ordinal (its linear
// (vchannel, partition) index) selects the shard by modulo, so the same
// bucket always lands in the same file. Fragments are coalesced into records
// of roughly spillRecordTargetBytes; the range's Logical and Rows carry the
// tail totals for sort-group packing and the fragment descriptor. Nil and
// empty batches are skipped; the caller only appends non-empty tails. The
// stream stays open across appends -- a range is a bare offset span, not a
// stream of its own.
func (l *SpillLog) Append(bucket int64, batches []SpillBatch) (SpillRange, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := int32(bucket % int64(len(l.streams)))
	s := l.streams[seq]
	if err := l.ensureStream(s); err != nil {
		return SpillRange{}, err
	}
	var rows, logical int64
	for _, batch := range batches {
		if batch.Data == nil || batch.Data.GetRowNum() == 0 {
			continue
		}
		rows += int64(batch.Data.GetRowNum())
		logical += batch.Bytes
	}
	begin := s.counted.n
	builder := array.NewRecordBuilder(memory.DefaultAllocator, l.arrow)
	defer builder.Release()
	var pending int64
	writeRecord := func() error {
		record := builder.NewRecord()
		defer record.Release()
		if record.NumRows() == 0 {
			return nil
		}
		if err := s.iw.Write(record); err != nil {
			return spillIOError(err, "write reshard spill file "+s.path)
		}
		pending = 0
		return nil
	}
	for _, batch := range batches {
		if batch.Data == nil || batch.Data.GetRowNum() == 0 {
			continue
		}
		if err := storage.BuildRecord(builder, batch.Data, l.schema); err != nil {
			return SpillRange{}, err
		}
		pending += batch.Bytes
		if pending >= spillRecordTargetBytes {
			if err := writeRecord(); err != nil {
				return SpillRange{}, err
			}
		}
	}
	if err := writeRecord(); err != nil {
		return SpillRange{}, err
	}
	s.live++
	return SpillRange{Stream: seq, Begin: begin, End: s.counted.n, Logical: logical, Rows: rows}, nil
}

// RangeReader replays one range as a storage.RecordReader compatible with
// storage.Sort/MergeSort: the run's captured schema message feeds the ipc
// reader's mandatory header, then the section supplies the range's record
// batches, and the section's bare EOF ends the stream cleanly. The fd is
// shared and cached per stream file; the returned reader borrows records
// until the next Next, exactly like the stream reader it replaces, and its
// Close never touches the shared fd.
func (l *SpillLog) RangeReader(r SpillRange) (storage.RecordReader, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fd, ok := l.readFds[r.Stream]
	if !ok {
		file, err := storage.Open(l.streams[r.Stream].path)
		if err != nil {
			return nil, err
		}
		fd = file
		l.readFds[r.Stream] = file
	}
	reader, err := ipc.NewReader(io.MultiReader(
		bytes.NewReader(l.schemaMsg),
		io.NewSectionReader(fd, r.Begin, r.End-r.Begin)))
	if err != nil {
		return nil, merr.Wrapf(err, "read reshard spill range [%d,%d) of file %s",
			r.Begin, r.End, l.streams[r.Stream].path)
	}
	return &spillRangeReader{reader: reader, field2Col: l.field2Col}, nil
}

// Release marks a bucket's ranges consumed; the detached write that replayed
// them calls it once its readers are closed. Because a shard only ever holds
// the tails of its fixed bucket group, a shard whose ranges are all released
// is removed immediately: a flushed bucket group reclaims its disk without
// waiting for unrelated buckets or for the run to end.
func (l *SpillLog) Release(ranges []SpillRange) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range ranges {
		s := l.streams[r.Stream]
		s.live--
		if s.live > 0 {
			continue
		}
		l.removeStream(r.Stream)
	}
}

// Close terminates every open stream with its EOS marker -- leaving each
// materialized file a valid standalone IPC stream for inspection -- and
// releases the append and cached read fds. The run joins its detached writes
// before calling this, so no reader is left with a borrowed fd. Files stay on
// disk: the run-level spill-root removal is the single cleanup owner.
func (l *SpillLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, s := range l.streams {
		if s.iw != nil {
			if err := s.iw.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			s.iw = nil
		}
		if s.file != nil {
			if err := s.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			s.file = nil
		}
	}
	for seq, fd := range l.readFds {
		if err := fd.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(l.readFds, seq)
	}
	return firstErr
}

// spillRangeReader replays one Arrow IPC range of a shared spill file.
type spillRangeReader struct {
	reader    *ipc.Reader
	field2Col map[storage.FieldID]int
}

var _ storage.RecordReader = (*spillRangeReader)(nil)

func (r *spillRangeReader) Next() (storage.Record, error) {
	if !r.reader.Next() {
		if err := r.reader.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	// The record is borrowed until the next Next; storage.Sort retains it.
	return storage.NewSimpleArrowRecord(r.reader.Record(), r.field2Col), nil
}

func (r *spillRangeReader) Close() error {
	if r == nil {
		return nil
	}
	r.reader.Release()
	return nil
}

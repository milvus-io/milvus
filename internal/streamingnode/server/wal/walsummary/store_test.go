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

package walsummary

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	return NewStore(cm, "by-dev-rootcoord-dml_0_40451v0", 1)
}

func testTransformRecord(timeTick uint64, pks ...int64) *streamingpb.VChannelSummaryTransformRecord {
	return &streamingpb.VChannelSummaryTransformRecord{
		TimeTick: timeTick,
		Delete: &streamingpb.TransformDeleteEntry{
			Blocks: []*streamingpb.TransformDeleteBlock{{
				PartitionId: 10,
				PrimaryKeys: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
				},
			}},
		},
	}
}

func TestMarshalUnmarshalChunkRoundTrip(t *testing.T) {
	records := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {testTransformRecord(100, 1, 2), testTransformRecord(102, 3)},
		"v2": {testTransformRecord(101, 4)},
	}
	payload, footer, err := marshalChunk("p1", 7, 1, records)
	assert.NoError(t, err)
	assert.NotNil(t, footer)
	assert.Equal(t, uint64(7), footer.GetGeneration())
	assert.Equal(t, int64(1), footer.GetTerm())
	assert.Equal(t, uint64(100), footer.GetStartTimetick())
	assert.Equal(t, uint64(102), footer.GetEndTimetick())
	assert.Len(t, footer.GetChunks(), 2)

	decoded, decodedFooter, err := unmarshalChunk(payload)
	assert.NoError(t, err)
	assert.Equal(t, footer.GetGeneration(), decodedFooter.GetGeneration())
	assert.Len(t, decoded, 2)
	assert.Equal(t, []uint64{100, 102}, timeticks(decoded["v1"]))
	assert.Equal(t, []uint64{101}, timeticks(decoded["v2"]))
	assert.Equal(t, int64(10), decoded["v1"][0].GetDelete().GetBlocks()[0].GetPartitionId())
}

func TestUnmarshalChunkCorrupted(t *testing.T) {
	records := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {testTransformRecord(100, 1)},
	}
	payload, _, err := marshalChunk("p1", 1, 1, records)
	assert.NoError(t, err)

	cases := map[string]func([]byte) []byte{
		"short payload": func(p []byte) []byte { return p[:10] },
		"bad header": func(p []byte) []byte {
			bad := append([]byte(nil), p...)
			copy(bad, "XXXXXXXX")
			return bad
		},
		"bad footer magic": func(p []byte) []byte {
			bad := append([]byte(nil), p...)
			copy(bad[len(bad)-4:], "XXXX")
			return bad
		},
		"bad checksum": func(p []byte) []byte {
			bad := append([]byte(nil), p...)
			bad[len(bad)-8] ^= 0xff
			return bad
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := unmarshalChunk(mutate(payload))
			assert.Error(t, err)
			assert.True(t, errors.Is(err, ErrStoreCorrupted))
		})
	}
}

func TestStoreWriteReadChunk(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	records := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {testTransformRecord(100, 1)},
	}
	footer, _, err := store.WriteChunk(ctx, 0, records)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), footer.GetGeneration())

	decoded, decodedFooter, err := store.ReadChunk(ctx, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, footer.GetGeneration(), decodedFooter.GetGeneration())
	assert.Equal(t, []uint64{100}, timeticks(decoded["v1"]))

	// identical rewrite is a no-op.
	_, _, err = store.WriteChunk(ctx, 0, records)
	assert.NoError(t, err)

	// different content at the same generation is corruption.
	_, _, err = store.WriteChunk(ctx, 0, map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {testTransformRecord(101, 1)},
	})
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrStoreCorrupted))
}

func TestStoreWriteChunkFencedByNewerTerm(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	records := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {testTransformRecord(100, 1)},
	}
	// Simulate a leftover chunk of a newer owner written under the stale
	// owner's key (split-brain residue): same generation key, footer term 5.
	payload, _, err := marshalChunk("p1", 7, 5, records)
	assert.NoError(t, err)
	assert.NoError(t, cm.Write(ctx, NewStore(cm, "p1", 3).ChunkKey(7), payload))

	// The stale owner (term 3) must not overwrite it.
	_, _, err = NewStore(cm, "p1", 3).WriteChunk(ctx, 7, records)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrStoreFenced))

	// A same-term rewrite with identical content stays idempotent even when
	// the stored bytes differ (a retry that spans a binary upgrade re-encodes
	// the same records differently). Flip a byte in the header's reserved
	// region, which no decoder checks, so bytes differ while content matches.
	ownPayload, _, err := marshalChunk("p1", 7, 3, records)
	assert.NoError(t, err)
	ownPayload[11] ^= 0xff
	assert.NoError(t, cm.Write(ctx, NewStore(cm, "p1", 3).ChunkKey(7), ownPayload))
	_, _, err = NewStore(cm, "p1", 3).WriteChunk(ctx, 7, records)
	assert.NoError(t, err)
}

func TestStoreManifestRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	_, found, err := store.ReadManifest(ctx)
	assert.NoError(t, err)
	assert.False(t, found)

	manifest := &streamingpb.PChannelSummaryManifest{
		Chunks: []*streamingpb.PChannelSummaryChunkIndexEntry{{
			Generation: 0,
			Term:       1,
			ObjectSize: 123,
		}},
		PendingGc: []*streamingpb.PChannelSummaryChunkRef{{
			Generation: 0,
			Term:       1,
		}},
	}
	assert.NoError(t, store.WriteManifest(ctx, manifest))

	loaded, found, err := store.ReadManifest(ctx)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Len(t, loaded.GetChunks(), 1)
	assert.Equal(t, uint64(0), loaded.GetChunks()[0].GetGeneration())
	assert.Len(t, loaded.GetPendingGc(), 1)
}

func TestStoreManifestCorrupted(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	assert.NoError(t, store.WriteManifest(ctx, &streamingpb.PChannelSummaryManifest{}))
	cm := store.chunkManager
	key := store.ManifestKey()
	payload, err := cm.Read(ctx, key)
	assert.NoError(t, err)
	payload[0] ^= 0xff
	assert.NoError(t, cm.Write(ctx, key, payload))
	_, _, err = store.ReadManifest(ctx)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrStoreCorrupted))
}

func TestStoreProbeChunkForward(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	records := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {testTransformRecord(100, 1)},
	}
	_, _, err := store.WriteChunk(ctx, 1, records)
	assert.NoError(t, err)
	_, _, err = store.WriteChunk(ctx, 2, records)
	assert.NoError(t, err)
	// probe from generation 2 finds only generation 2.
	entries, err := store.ProbeChunkForward(ctx, 2)
	assert.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, uint64(2), entries[0].GetGeneration())
	// probe from 0 finds both, in order.
	entries, err = store.ProbeChunkForward(ctx, 0)
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Equal(t, uint64(1), entries[0].GetGeneration())
	assert.Equal(t, uint64(2), entries[1].GetGeneration())
}

func TestStoreDeleteChunk(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	records := map[string][]*streamingpb.VChannelSummaryTransformRecord{
		"v1": {testTransformRecord(100, 1)},
	}
	_, _, err := store.WriteChunk(ctx, 0, records)
	assert.NoError(t, err)
	assert.NoError(t, store.DeleteChunk(ctx, 0, 1))
	_, _, err = store.ReadChunk(ctx, 0, 1)
	assert.Error(t, err)
	// deleting again is a no-op (missing object tolerated).
	assert.NoError(t, store.DeleteChunk(ctx, 0, 1))
}

func TestInheritManifest(t *testing.T) {
	previous := &streamingpb.PChannelSummaryManifest{
		Chunks:    []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 0}},
		PendingGc: []*streamingpb.PChannelSummaryChunkRef{{Generation: 0}},
	}
	discovered := []*streamingpb.PChannelSummaryChunkIndexEntry{
		{Generation: 2},
		{Generation: 1},
	}
	manifest := inheritManifest(previous, discovered)
	assert.Len(t, manifest.GetChunks(), 3)
	assert.Equal(t, uint64(0), manifest.GetChunks()[0].GetGeneration())
	assert.Equal(t, uint64(1), manifest.GetChunks()[1].GetGeneration())
	assert.Equal(t, uint64(2), manifest.GetChunks()[2].GetGeneration())
	// a discovered duplicate is skipped.
	manifest = inheritManifest(manifest, []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 1}})
	assert.Len(t, manifest.GetChunks(), 3)
}

func TestChunkKeySanitize(t *testing.T) {
	store := newTestStore(t)
	key := store.ChunkKey(3)
	assert.Contains(t, key, "/chunks/00000000000000000003_00000000000000000001")
	manifestKey := store.ManifestKey()
	assert.Contains(t, manifestKey, "/manifest/00000000000000000001")
}

func timeticks(records []*streamingpb.VChannelSummaryTransformRecord) []uint64 {
	tts := make([]uint64, 0, len(records))
	for _, record := range records {
		tts = append(tts, record.GetTimeTick())
	}
	return tts
}

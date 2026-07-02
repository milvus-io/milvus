package wp

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(message.WALNameWoodpecker)
	assert.NotNil(t, registeredB)
	assert.Equal(t, message.WALNameWoodpecker, registeredB.Name())

	id, err := message.UnmarshalMessageID(&commonpb.MessageID{
		WALName: commonpb.WALName(message.WALNameWoodpecker),
		Id:      newMessageIDOfWoodpecker(1, 2).Marshal(),
	})
	assert.NoError(t, err)
	assert.True(t, id.EQ(newMessageIDOfWoodpecker(1, 2)))
}

func TestWAL(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWpWAL")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "minio", // Using default storage type minio-compatible
			rootPath:    "",      // No need to specify path for this storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",             // Using default storage type minio-compatible
			rootPath:    rootPath + "_service", // No need to specify path for this storage
			needCluster: true,
		},
	}
	wpBackendTypeKey := paramtable.Get().WoodpeckerCfg.StorageType.Key
	wpBackendRootPathKey := paramtable.Get().WoodpeckerCfg.RootPath.Key
	wpPoolKey := paramtable.Get().WoodpeckerCfg.QuorumBufferPools.Key
	debugKey := paramtable.Get().LogCfg.Level.Key
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// set params
			err := paramtable.Get().Save(wpBackendTypeKey, tc.storageType)
			assert.NoError(t, err)
			err = paramtable.Get().Save(wpBackendRootPathKey, tc.rootPath)
			assert.NoError(t, err)

			// startup cluster if need
			if tc.needCluster {
				paramtable.Get().Save(debugKey, "debug")
				// get default cfg
				cfg, err := config.NewConfiguration()
				assert.NoError(t, err)
				err = setCustomWpConfig(cfg, &paramtable.Get().WoodpeckerCfg)
				assert.NoError(t, err)
				// setup mini cluster
				const nodeCount = 3
				cluster, cfg, _, serviceSeeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, serviceSeeds)
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()
				// set back config using miniCluster seeds
				testPools := []config.QuorumBufferPool{
					{
						Name:  "defaultpool",
						Seeds: serviceSeeds,
					},
				}
				jsonBytes, err := json.Marshal(testPools)
				assert.NoError(t, err)
				jsonStr := string(jsonBytes)
				saveErr := paramtable.Get().Save(wpPoolKey, jsonStr)
				assert.NoError(t, saveErr)
			} else {
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}

			// run test
			walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
		})
	}
}

// TestWpRetentionTruncateRead verifies the read semantics around woodpecker truncation.
//
// Milvus truncates the WAL aggressively (right after a flush), but truncation only
// moves a metadata watermark -- it does NOT delete data. Data below the watermark
// stays readable until the retention TTL GC physically removes it. This test pins that
// with a large retention (so the GC path never fires) plus a small segment-rolling size
// (so writes span several woodpecker segments), and covers:
//
//	TruncatedDataStillReadable: after truncating to the latest id `b`, reading from any
//	  explicit position in (earliest, b] -- across segment boundaries -- still returns
//	  every message; truncation erased nothing.
//	SeekForwardWithinSegment / SeekForwardCrossSegment: a reader that asks for data older
//	  than the earliest deliverable position (DeliverPolicy_All after truncation) is moved
//	  forward to the first available position instead of erroring or stalling. The two
//	  sub-tests exercise both branches of adjustPendingReadPointIfTruncated -- a
//	  truncation point inside segment 0 vs. one in a later segment.
//
// One opener is shared by all sub-tests (rolling size is read once at Build time), so the
// test builds the embed LogStore exactly once.
func TestWpRetentionTruncateRead(t *testing.T) {
	ctx := context.Background()
	pt := paramtable.Get()

	// Large retention so nothing is GC'd during the test: this isolates the "truncate
	// moves a watermark, it does not delete data" behavior from the separate retention-TTL
	// GC path. Local fs storage keeps it fast and dependency-light; the truncate/read-
	// adjust logic lives in woodpecker's log handle/reader and is storage-backend agnostic.
	require.NoError(t, pt.Save(pt.WoodpeckerCfg.RetentionTTL.Key, "72h"))
	require.NoError(t, pt.Save(pt.WoodpeckerCfg.StorageType.Key, "local"))
	require.NoError(t, pt.Save(pt.WoodpeckerCfg.RootPath.Key, filepath.Join(t.TempDir(), "wp_ret_trunc")))
	// Small rolling size + padded messages => a new segment every few messages
	// (woodpecker rolls by flushed byte size, not timing), so the seek-forward sub-tests
	// can reach both the within-segment and cross-segment adjustment branches.
	require.NoError(t, pt.Save(pt.WoodpeckerCfg.SegmentRollingMaxSize.Key, "4096"))

	o, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	require.NotNil(t, o)
	defer func() {
		o.Close()
		// stop embed LogStore singleton (local/minio mode), mirroring TestWAL.
		assert.NoError(t, woodpecker.StopEmbedLogStore(), "close embed LogStore instance error")
	}()

	const (
		msgCount = 30
		padBytes = 1024 // with maxSize=4096 => ~3 messages per segment
	)
	segOf := func(id message.MessageID) int64 { return id.(wpID).WoodpeckerID().SegmentId }

	// Behavior 1: data at/under the truncation watermark is still readable while it has
	// not been GC'd. Truncate to the latest id, then read from several explicit positions
	// in (earliest, latest] -- spanning segment boundaries -- and confirm every message
	// comes back.
	t.Run("TruncatedDataStillReadable", func(t *testing.T) {
		w := openWpRWWAL(t, ctx, o, uniqueWpPChannel("ret_trunc_b1"))
		defer w.Close()
		ids := appendWpSeq(t, ctx, w, msgCount, padBytes)
		require.Greaterf(t, segOf(ids[msgCount-1]), int64(0),
			"expected writes to span >=2 segments, but last id is in segment %d", segOf(ids[msgCount-1]))

		// b = latest. Truncate is inclusive, so the whole WAL is at/under the watermark.
		require.NoError(t, w.Truncate(ctx, ids[msgCount-1]))

		// k=0 is intentionally skipped: ids[0] == EarliestLogMessageID() ({0,0}), which
		// woodpecker treats like DeliverPolicy_All and would adjust past the watermark.
		// Every other StartFrom position is read verbatim (not adjusted), across segments,
		// including the watermark id itself (k=msgCount-1).
		for _, k := range []int{1, msgCount / 2, msgCount - 1} {
			readExpectWpIDs(t, ctx, w, fmt.Sprintf("b1_startfrom_%d", k),
				options.DeliverPolicyStartFrom(ids[k]), ids[k:])
		}

		// StartAfter(k) delivers ids[k+1:].
		k := msgCount / 2
		readExpectWpIDs(t, ctx, w, "b1_startafter",
			options.DeliverPolicyStartAfter(ids[k]), ids[k+1:])

		// Contrast that proves the truncate actually moved the watermark (and that this
		// sub-test is not just reading an un-truncated WAL): DeliverPolicy_All starts at
		// EarliestLogMessageID() and is adjusted past the watermark (= the latest id
		// here), so a fresh "all" reader is parked at the end and sees nothing -- while
		// the explicit StartFrom reads above still return everything below the watermark.
		assertWpNoMessage(t, ctx, w, "b1_all_parked_after_truncate",
			options.DeliverPolicyAll(), 2*time.Second)
	})

	// Behavior 2a (within-segment): the truncation point is in segment 0, so the open-time
	// adjustment only bumps the entry id (truncatedEntryId+1) inside that same segment.
	// A DeliverPolicy_All reader resumes at ids[j+1] instead of erroring or stalling.
	t.Run("SeekForwardWithinSegment", func(t *testing.T) {
		w := openWpRWWAL(t, ctx, o, uniqueWpPChannel("ret_trunc_b2a"))
		defer w.Close()
		ids := appendWpSeq(t, ctx, w, msgCount, padBytes)

		// Truncate at the last message still in segment 0, ensuring data after it lives in
		// a later segment.
		j := 0
		for j+1 < msgCount && segOf(ids[j+1]) == 0 {
			j++
		}
		require.Equalf(t, int64(0), segOf(ids[j]), "truncation point ids[%d] must be in segment 0", j)
		require.Lessf(t, j, msgCount-1, "need data after the truncation point ids[%d]", j)

		require.NoError(t, w.Truncate(ctx, ids[j]))
		readExpectWpIDs(t, ctx, w, "b2a_all_seek_forward",
			options.DeliverPolicyAll(), ids[j+1:])
	})

	// Behavior 2b (cross-segment): the truncation point is in a later segment, so the
	// open-time adjustment first jumps the segment id up to the truncation segment (the
	// branch a single-segment WAL can never reach), then to truncatedEntryId+1. The
	// precondition asserts the truncation point really is in segment > 0, so this coverage
	// cannot silently rot if message sizes change.
	t.Run("SeekForwardCrossSegment", func(t *testing.T) {
		w := openWpRWWAL(t, ctx, o, uniqueWpPChannel("ret_trunc_b2b"))
		defer w.Close()
		ids := appendWpSeq(t, ctx, w, msgCount, padBytes)

		// Pick a middle truncation point that lies in a segment > 0.
		j := msgCount / 2
		for j < msgCount-1 && segOf(ids[j]) == 0 {
			j++
		}
		require.Greaterf(t, segOf(ids[j]), int64(0),
			"truncation point ids[%d] must be in segment >0 to hit the cross-segment branch", j)
		require.Lessf(t, j, msgCount-1, "need data after the truncation point ids[%d]", j)

		require.NoError(t, w.Truncate(ctx, ids[j]))
		// DeliverPolicy_All resumes exactly at ids[j+1] (via the cross-segment branch) and
		// streams to the end.
		readExpectWpIDs(t, ctx, w, "b2b_all_seek_forward",
			options.DeliverPolicyAll(), ids[j+1:])
		// A cross-segment StartFrom also returns truncated-but-present data from the
		// truncation point through the remaining segments.
		readExpectWpIDs(t, ctx, w, "b2b_startfrom",
			options.DeliverPolicyStartFrom(ids[j]), ids[j:])
	})
}

// uniqueWpPChannel returns a collision-free pchannel name so each run uses a fresh
// woodpecker log: log names persist in etcd metadata, and reusing one would read stale
// data from a previous run.
func uniqueWpPChannel(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// openWpRWWAL opens a read-write WAL on a fresh pchannel.
func openWpRWWAL(t *testing.T, ctx context.Context, o walimpls.OpenerImpls, name string) walimpls.WALImpls {
	w, err := o.Open(ctx, &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       name,
			Term:       1,
			AccessMode: types.AccessModeRW,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, w)
	return w
}

// appendWpSeq appends n messages sequentially and returns their ids in append order.
// A single-threaded writer gets strictly increasing woodpecker ids, so the returned
// slice is sorted and equals the order a reader will deliver them. padBytes adds a
// padding property of that size to each message: callers use it together with a small
// SegmentRollingMaxSize to deterministically roll multiple woodpecker segments.
func appendWpSeq(t *testing.T, ctx context.Context, w walimpls.WALImpls, n, padBytes int) []message.MessageID {
	ids := make([]message.MessageID, 0, n)
	for i := 0; i < n; i++ {
		props := map[string]string{}
		if padBytes > 0 {
			props["pad"] = strings.Repeat("x", padBytes)
		}
		id, err := w.Append(ctx, message.CreateTestEmptyInsertMesage(int64(i), props))
		require.NoError(t, err)
		require.NotNil(t, id)
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		require.Truef(t, ids[i-1].LT(ids[i]), "woodpecker ids must be monotonically increasing: %v !< %v", ids[i-1], ids[i])
	}
	return ids
}

// readExpectWpIDs opens a scanner with the given policy and asserts it delivers exactly
// expected (in order), then closes it. It reads precisely len(expected) messages rather
// than waiting for channel close, because a wp scanner is a tail reader that blocks
// after the last available message.
func readExpectWpIDs(t *testing.T, ctx context.Context, w walimpls.WALImpls, name string, policy options.DeliverPolicy, expected []message.MessageID) {
	s, err := w.Read(ctx, walimpls.ReadOption{
		Name:          name,
		DeliverPolicy: policy,
	})
	require.NoError(t, err)
	defer s.Close()

	for i, want := range expected {
		select {
		case msg, ok := <-s.Chan():
			require.Truef(t, ok, "%s: scanner channel closed early at message %d/%d", name, i, len(expected))
			require.NotNil(t, msg)
			assert.Truef(t, msg.MessageID().EQ(want), "%s: message %d/%d got %v want %v", name, i, len(expected), msg.MessageID(), want)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s: timed out waiting for message %d/%d", name, i, len(expected))
		}
	}
}

// assertWpNoMessage opens a scanner with the given policy and asserts that no message is
// delivered within d. Used to show a reader is parked past all available data (e.g. a
// DeliverPolicy_All reader after the WAL was truncated up to its latest id).
func assertWpNoMessage(t *testing.T, ctx context.Context, w walimpls.WALImpls, name string, policy options.DeliverPolicy, d time.Duration) {
	s, err := w.Read(ctx, walimpls.ReadOption{
		Name:          name,
		DeliverPolicy: policy,
	})
	require.NoError(t, err)
	defer s.Close()

	select {
	case msg, ok := <-s.Chan():
		if ok {
			t.Fatalf("%s: expected no message but got %v", name, msg.MessageID())
		}
	case <-time.After(d):
	}
}

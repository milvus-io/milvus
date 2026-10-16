package compaction

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	cerrors "github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// writeTable writes one table with the given key/value pairs, in key order,
// and opens it.
func writeTable(t *testing.T, dir string, id int64, kv ...string) *sst.Reader {
	t.Helper()
	w, err := sst.NewWriter(dir, sst.ID(id))
	require.NoError(t, err)
	for i := 0; i+1 < len(kv); i += 2 {
		require.NoError(t, w.Add([]byte(kv[i]), []byte(kv[i+1])))
	}
	_, err = w.Close()
	require.NoError(t, err)
	r, err := sst.OpenReader(filepath.Join(dir, sst.FileName(sst.ID(id))), nil)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	return r
}

type memOutput struct{ kv [][2]string }

func (o *memOutput) Add(key, value []byte) error {
	o.kv = append(o.kv, [2]string{string(key), string(value)})
	return nil
}

func TestMergeLastWriterWins(t *testing.T) {
	dir := t.TempDir()
	tomb := string(codec.EncodeTombstone())
	newest := writeTable(t, dir, 3, "b", "b3", "c", tomb)
	middle := writeTable(t, dir, 2, "a", "a2", "b", "b2")
	oldest := writeTable(t, dir, 1, "a", "a1", "c", "c1", "d", "d1")

	out := &memOutput{}
	stats, err := Merge(context.Background(), []Source{newest, middle, oldest}, nil, false, out, nil)
	require.NoError(t, err)
	require.Equal(t, [][2]string{{"a", "a2"}, {"b", "b3"}, {"c", tomb}, {"d", "d1"}}, out.kv)
	require.Equal(t, Stats{Read: 7, Dropped: 3, Written: 4}, stats)

	out = &memOutput{}
	stats, err = Merge(context.Background(), []Source{newest, middle, oldest}, nil, true, out, nil)
	require.NoError(t, err)
	require.Equal(t, [][2]string{{"a", "a2"}, {"b", "b3"}, {"d", "d1"}}, out.kv)
	require.Equal(t, Stats{Read: 7, Dropped: 4, Written: 3}, stats)
}

func TestMergeEmptyAndSingleSources(t *testing.T) {
	dir := t.TempDir()
	empty := writeTable(t, dir, 5)
	one := writeTable(t, dir, 6, "k", "v")
	out := &memOutput{}
	stats, err := Merge(context.Background(), []Source{empty, one}, nil, false, out, nil)
	require.NoError(t, err)
	require.Equal(t, [][2]string{{"k", "v"}}, out.kv)
	require.Equal(t, Stats{Read: 1, Written: 1}, stats)
	stats, err = Merge(context.Background(), nil, nil, false, &memOutput{}, nil)
	require.NoError(t, err)
	require.Equal(t, Stats{}, stats)
}

type keepUpper struct{}

func (keepUpper) Apply(key, value []byte) ([]byte, bool, error) {
	if string(key) == "d" {
		return nil, false, nil
	}
	return append([]byte("X"), value...), true, nil
}

func TestMergeAppliesOperators(t *testing.T) {
	dir := t.TempDir()
	a := writeTable(t, dir, 1, "a", "1", "d", "4")
	out := &memOutput{}
	stats, err := Merge(context.Background(), []Source{a}, []Operator{keepUpper{}}, false, out, nil)
	require.NoError(t, err)
	require.Equal(t, [][2]string{{"a", "X1"}}, out.kv)
	require.Equal(t, Stats{Read: 2, Dropped: 1, Written: 1}, stats)
}

func TestMergeStopsOnCancel(t *testing.T) {
	dir := t.TempDir()
	a := writeTable(t, dir, 1, "a", "1", "b", "2")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Merge(ctx, []Source{a}, nil, false, &memOutput{}, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSplitOutputCutsAtTargetSize(t *testing.T) {
	dir := t.TempDir()
	var closed []sst.Info
	o := &splitOutput{dir: dir, nextID: 10, endID: 100, maxBytes: 8 << 10, onClose: func(i sst.Info) error { closed = append(closed, i); return nil }}
	rng := rand.New(rand.NewSource(1))
	val := make([]byte, 1024)
	for i := 0; i < 20; i++ {
		rng.Read(val) // random values do not compress
		require.NoError(t, o.Add([]byte(fmt.Sprintf("k%04d", i)), val))
	}
	infos, err := o.Finish()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(infos), 2)
	require.LessOrEqual(t, len(infos), 5)
	require.Equal(t, infos, closed)
	var entries int64
	for i, info := range infos {
		require.Equal(t, sst.ID(10+i), info.ID)
		entries += info.NumEntries
		if i < len(infos)-1 {
			require.GreaterOrEqual(t, info.Size, int64(4<<10))
		}
	}
	require.EqualValues(t, 20, entries)
	require.Equal(t, "k0000", string(infos[0].MinKey))

	o = &splitOutput{dir: dir, nextID: 30, endID: 30, maxBytes: 8}
	require.ErrorIs(t, o.Add([]byte("a"), []byte("v")), errOutputIDsExhausted)

	// an output with no entries writes no table
	o = &splitOutput{dir: dir, nextID: 40, endID: 41, maxBytes: 8}
	infos, err = o.Finish()
	require.NoError(t, err)
	require.Empty(t, infos)
	require.Error(t, o.Add([]byte("a"), []byte("v"))) // after Finish

	o = &splitOutput{dir: dir, nextID: 50, endID: 51, maxBytes: 8}
	require.NoError(t, o.Add([]byte("a"), []byte("v")))
	o.Abort()
	_, err = o.Finish()
	require.Error(t, err)
}

func TestSplitOutputOnCloseFailure(t *testing.T) {
	boom := cerrors.New("boom")
	o := &splitOutput{dir: t.TempDir(), nextID: 1, endID: 9, maxBytes: 1, onClose: func(sst.Info) error { return boom }}
	require.ErrorIs(t, o.Add([]byte("a"), []byte("v")), boom)
}

type failOp struct{}

func (failOp) Apply(key, value []byte) ([]byte, bool, error) {
	return nil, false, cerrors.New("op failed")
}

func TestMergePropagatesOperatorError(t *testing.T) {
	a := writeTable(t, t.TempDir(), 1, "a", "1")
	_, err := Merge(context.Background(), []Source{a}, []Operator{failOp{}}, false, &memOutput{}, nil)
	require.ErrorContains(t, err, "op failed")
}

func TestMergeReportsCorruptTable(t *testing.T) {
	dir := t.TempDir()
	w, err := sst.NewWriter(dir, 8)
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		require.NoError(t, w.Add([]byte(fmt.Sprintf("k%06d", i)), []byte("value")))
	}
	_, err = w.Close()
	require.NoError(t, err)
	path := filepath.Join(dir, sst.FileName(8))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	off := len(b) / 4 // inside the data blocks, well before the index and footer
	for i := off; i < off+64; i++ {
		b[i] ^= 0xff
	}
	require.NoError(t, os.WriteFile(path, b, 0o600))
	r, err := sst.OpenReader(path, nil)
	require.NoError(t, err)
	defer r.Close()
	_, err = Merge(context.Background(), []Source{r}, nil, false, &memOutput{}, nil)
	require.Error(t, err)
	require.True(t, cerrors.Is(err, pkindex.ErrCorrupted), "got %v", err)
}

type fakeStore struct {
	mu       sync.Mutex
	uploaded []string
	failKey  string
	block    chan struct{} // when set, UploadFile waits on it
}

func (s *fakeStore) UploadFile(_ context.Context, key, _ string) error {
	if s.block != nil {
		<-s.block
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == s.failKey {
		return cerrors.New("upload failed")
	}
	s.uploaded = append(s.uploaded, key)
	return nil
}

func (s *fakeStore) DownloadFile(context.Context, string, string) error { return nil }

func touchTable(t *testing.T, dir string, id sst.ID) string {
	t.Helper()
	p := filepath.Join(dir, sst.FileName(id))
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
	return p
}

func TestUploaderUploadsAndRemovesLocalFiles(t *testing.T) {
	dir := t.TempDir()
	p1, p2 := touchTable(t, dir, 1), touchTable(t, dir, 2)
	store := &fakeStore{}
	u := startUploader(context.Background(), store, "base", dir)
	require.NoError(t, u.enqueue(sst.Info{ID: 1}))
	require.NoError(t, u.enqueue(sst.Info{ID: 2}))
	require.NoError(t, u.finish())
	require.Equal(t, []string{pkindex.SSTKey("base", 1), pkindex.SSTKey("base", 2)}, store.uploaded)
	require.NoFileExists(t, p1)
	require.NoFileExists(t, p2)
}

func TestUploaderKeepsFirstFailure(t *testing.T) {
	dir := t.TempDir()
	touchTable(t, dir, 1)
	touchTable(t, dir, 2)
	store := &fakeStore{failKey: pkindex.SSTKey("base", 1)}
	u := startUploader(context.Background(), store, "base", dir)
	require.NoError(t, u.enqueue(sst.Info{ID: 1}))
	// the failure shows once the upload ran; wait for it
	require.Eventually(t, func() bool { return u.failure() != nil }, 5e9, 1e6)
	require.Error(t, u.enqueue(sst.Info{ID: 2}))
	require.Error(t, u.finish())
	require.Empty(t, store.uploaded)
}

func TestUploaderEnqueueAfterFinishAndOnCancel(t *testing.T) {
	dir := t.TempDir()
	u := startUploader(context.Background(), &fakeStore{}, "base", dir)
	require.NoError(t, u.finish())
	require.Error(t, u.enqueue(sst.Info{ID: 1}))

	// the store blocks: one table is in upload, one fills the queue, the next waits
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeStore{block: make(chan struct{})}
	u = startUploader(ctx, store, "base", dir)
	touchTable(t, dir, 1)
	touchTable(t, dir, 2)
	require.NoError(t, u.enqueue(sst.Info{ID: 1}))
	require.Eventually(t, func() bool { return len(u.ch) == 0 }, 5*time.Second, time.Millisecond)
	require.NoError(t, u.enqueue(sst.Info{ID: 2}))
	cancel()
	require.ErrorIs(t, u.enqueue(sst.Info{ID: 3}), context.Canceled)
	close(store.block)
	require.NoError(t, u.finish())
}

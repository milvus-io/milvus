package parquet

import (
	"context"
	"encoding/binary"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type readerAtFunc func(p []byte, off int64) (int, error)

func (f readerAtFunc) ReadAt(p []byte, off int64) (int, error) { return f(p, off) }

// A hostile parquet declares a footer as large as the object itself. Arrow's only
// check is that the declared length fits inside the file, after which it allocates
// it, and SizingReaderAt buffers the ranged GET on top -- so sizing must reject the
// length rather than pay for it. Measured by allocation, since the defect is memory
// and not the (already failing) parse.
func Test_NumRows_hostileFooterLength(t *testing.T) {
	const objSize = int64(200 << 20)
	declared := uint32(objSize - 8)

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, "bad.parquet").Return(objSize, nil).Maybe()
	cm.EXPECT().ReadAt(mock.Anything, "bad.parquet", mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, off int64, length int64) ([]byte, error) {
			b := make([]byte, length)
			if off+length == objSize && length >= 8 {
				binary.LittleEndian.PutUint32(b[length-8:], declared)
				copy(b[length-4:], magic)
			}
			return b, nil
		}).Maybe()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := NumRows(context.Background(), cm, "bad.parquet")
	runtime.ReadMemStats(&after)

	require.Error(t, err)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(footerMaxSize()),
		"sizing must reject the declared footer length instead of allocating it")
}

func Test_validateFooter(t *testing.T) {
	tail := func(footerLen uint32, m []byte) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint32(b[:4], footerLen)
		copy(b[4:], m)
		return b
	}
	readerAt := func(tail []byte, size int64) io.ReaderAt {
		return readerAtFunc(func(p []byte, off int64) (int, error) {
			if off != size-8 {
				return 0, io.EOF
			}
			return copy(p, tail), nil
		})
	}

	t.Run("accepts a plausible footer", func(t *testing.T) {
		assert.NoError(t, validateFooter(readerAt(tail(4096, magic), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("accepts an encrypted footer", func(t *testing.T) {
		assert.NoError(t, validateFooter(readerAt(tail(4096, magicEncrypted), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("rejects a footer over the cap", func(t *testing.T) {
		err := validateFooter(readerAt(tail(uint32(footerMaxSize())+1, magic), 1<<30), 1<<30, "a.parquet")
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
	t.Run("rejects a zero-length footer", func(t *testing.T) {
		assert.Error(t, validateFooter(readerAt(tail(0, magic), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("rejects a foreign magic", func(t *testing.T) {
		assert.Error(t, validateFooter(readerAt(tail(4096, []byte("XXXX")), 1<<20), 1<<20, "a.parquet"))
	})
	t.Run("rejects a file too small to hold a footer", func(t *testing.T) {
		assert.Error(t, validateFooter(readerAt(nil, 4), 4, "a.parquet"))
	})
}

func Test_NumRows_boundsConcurrentFooterParses(t *testing.T) {
	// footerMaxSize bounds the bytes read, not what Arrow's thrift decoder
	// allocates from them, so the number of decodes in flight is what caps the
	// coordinator's exposure. The gate wraps only the decode -- the ranged reads
	// before it are ordinary storage traffic -- so concurrency is measured on the
	// footer read the decoder issues, not on the 8-byte tail read that
	// validateFooter does ahead of the gate.
	const objSize = int64(1 << 20)
	const declaredFooterLen = uint32(4096)
	var inFlight, peak, tailReads atomic.Int32

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(objSize, nil).Maybe()
	cm.EXPECT().ReadAt(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, off int64, length int64) ([]byte, error) {
			b := make([]byte, length)
			if off+length == objSize && length == 8 {
				// The tail probe: hand back a well-formed footer descriptor so the
				// call proceeds past validateFooter and into the decode.
				tailReads.Add(1)
				binary.LittleEndian.PutUint32(b[:4], declaredFooterLen)
				copy(b[4:], magic)
				return b, nil
			}
			cur := inFlight.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			inFlight.Add(-1)
			// Garbage where a footer should be: the decode fails, inside the gate.
			return b, nil
		}).Maybe()

	var wg sync.WaitGroup
	for i := 0; i < 4*maxConcurrentFooterParses; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := NumRows(context.Background(), cm, "f.parquet")
			assert.Error(t, err)
		}()
	}
	wg.Wait()

	assert.Positive(t, tailReads.Load(), "the probe must have passed footer validation")
	assert.LessOrEqual(t, peak.Load(), int32(maxConcurrentFooterParses),
		"concurrent footer decodes must stay within the process-wide cap")
	assert.Greater(t, peak.Load(), int32(1), "the probe must actually have overlapped")
}

func Test_validateFooter_capIsConfigurable(t *testing.T) {
	// The cap is stricter than the DataNode reader, which bounds the footer only
	// by file size, so a legitimate file with a large footer would be refused at
	// submit with no way out. It is configurable for exactly that reason.
	paramtable.Init()
	key := paramtable.Get().DataCoordCfg.ImportParquetFooterMaxSize.Key

	const declared = uint32(32 << 20) // above the old fixed 16 MiB cap
	probe := func() error {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint32(b[:4], declared)
		copy(b[4:], magic)
		const size = int64(1) << 30
		ra := readerAtFunc(func(p []byte, off int64) (int, error) {
			if off == size-8 {
				return copy(p, b), nil
			}
			return 0, io.EOF
		})
		return validateFooter(ra, size, "a.parquet")
	}

	paramtable.Get().Save(key, "16777216")
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	require.Error(t, probe(), "a 32 MiB footer must be refused under a 16 MiB cap")

	paramtable.Get().Save(key, "67108864")
	assert.NoError(t, probe(), "the same footer must pass under the 64 MiB default")

	paramtable.Get().Save(key, "0")
	assert.NoError(t, probe(), "a nonsensical value falls back to the default rather than refusing everything")
}

package extends

import "github.com/milvus-io/milvus/internal/lognode/server/wal"

// ScannerWithCleanup wraps a scanner with cleanup function.
func ScannerWithCleanup(s wal.Scanner, cleanup func()) wal.Scanner {
	return &scannerWithCleanup{
		Scanner: s,
		cleanup: cleanup,
	}
}

// scannerWithCleanup is a wrapper of scanner to add cleanup function.
type scannerWithCleanup struct {
	wal.Scanner
	cleanup func()
}

// scannerCleanupWrapper overrides Scanner Close function.
func (sw *scannerWithCleanup) Close() error {
	err := sw.Scanner.Close()
	sw.cleanup()
	return err
}

// WALWithCleanup wraps a wal with cleanup function.
func WALWithCleanup(w wal.WAL, cleanup func()) wal.WAL {
	return &walWithCleanup{
		WAL:     w,
		cleanup: cleanup,
	}
}

// walWithCleanup is a wrapper of wal to add cleanup function.
type walWithCleanup struct {
	wal.WAL
	cleanup func()
}

// walCleanupWrapper overrides wal Close function.
func (sw *walWithCleanup) Close() {
	sw.WAL.Close()
	sw.cleanup()
}

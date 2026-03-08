package helper

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// NewScannerHelper creates a new ScannerHelper.
func NewScannerHelper(scannerName string) *ScannerHelper {
	return &ScannerHelper{
		scannerName: scannerName,
		notifier:    syncutil.NewAsyncTaskNotifier[error](),
	}
}

// ScannerHelper is a helper for scanner implementation.
type ScannerHelper struct {
	scannerName string
	notifier    *syncutil.AsyncTaskNotifier[error]
}

// Context returns the context of the scanner, which will cancel when the scanner helper is closed.
func (s *ScannerHelper) Context() context.Context {
	return s.notifier.Context()
}

// Name returns the name of the scanner.
func (s *ScannerHelper) Name() string {
	return s.scannerName
}

// Error returns the error of the scanner.
func (s *ScannerHelper) Error() error {
	return s.notifier.BlockAndGetResult()
}

// Done returns a channel that will be closed when the scanner is finished.
func (s *ScannerHelper) Done() <-chan struct{} {
	return s.notifier.FinishChan()
}

// Close closes the scanner, block until the Finish is called.
func (s *ScannerHelper) Close() error {
	s.notifier.Cancel()
	return s.notifier.BlockAndGetResult()
}

// Finish finishes the scanner with an error.
func (s *ScannerHelper) Finish(err error) {
	s.notifier.Finish(err)
}

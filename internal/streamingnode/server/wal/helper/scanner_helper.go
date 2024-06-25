package helper

import "context"

// NewScannerHelper creates a new ScannerHelper.
func NewScannerHelper(scannerName string) *ScannerHelper {
	ctx, cancel := context.WithCancel(context.Background())
	return &ScannerHelper{
		scannerName: scannerName,
		ctx:         ctx,
		cancel:      cancel,
		finishCh:    make(chan struct{}),
		err:         nil,
	}
}

// ScannerHelper is a helper for scanner implementation.
type ScannerHelper struct {
	scannerName string
	ctx         context.Context
	cancel      context.CancelFunc
	finishCh    chan struct{}
	err         error
}

// Context returns the context of the scanner, which will cancel when the scanner helper is closed.
func (s *ScannerHelper) Context() context.Context {
	return s.ctx
}

// Name returns the name of the scanner.
func (s *ScannerHelper) Name() string {
	return s.scannerName
}

// Error returns the error of the scanner.
func (s *ScannerHelper) Error() error {
	<-s.finishCh
	return s.err
}

// Done returns a channel that will be closed when the scanner is finished.
func (s *ScannerHelper) Done() <-chan struct{} {
	return s.finishCh
}

// Close closes the scanner, block until the Finish is called.
func (s *ScannerHelper) Close() error {
	s.cancel()
	<-s.finishCh
	return s.err
}

// Finish finishes the scanner with an error.
func (s *ScannerHelper) Finish(err error) {
	s.err = err
	close(s.finishCh)
}

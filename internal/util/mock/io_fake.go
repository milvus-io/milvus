package mock

type ErrReader struct {
	Err error
}

func (r *ErrReader) Read(p []byte) (n int, err error) {
	return 0, r.Err
}

type ErrWriter struct {
	Err error
}

func (w *ErrWriter) Write(p []byte) (n int, err error) {
	return 0, w.Err
}

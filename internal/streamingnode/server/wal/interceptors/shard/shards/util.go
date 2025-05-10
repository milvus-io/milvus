package shards

type TxnManager interface {
	RecoverDone() <-chan struct{}
}

// TxnSession is a session interface
type TxnSession interface {
	// should be called when the session is done.
	RegisterCleanup(cleanup func(), timetick uint64)
}

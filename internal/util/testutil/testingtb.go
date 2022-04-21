package testutil

// TB is a subset of methods of testing.TB interface.
// We cannot implement testing.TB due to protection, so we expose this simplified interface.
type TB interface {
	Cleanup(func())
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fail()
	FailNow()
	Failed() bool
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Logf(format string, args ...interface{})
	Name() string
	TempDir() string
	Helper()
	Skip(args ...interface{})
}

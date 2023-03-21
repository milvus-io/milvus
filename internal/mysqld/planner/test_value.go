package planner

type TestValue int

const (
	TestValueUnknown TestValue = iota
	TestValueTrue
	TestValueFalse
)

func (t TestValue) String() string {
	switch t {
	case TestValueTrue:
		return "true"
	case TestValueFalse:
		return "false"
	default:
		return "unknown"
	}
}

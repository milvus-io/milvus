package planparserv2

const float64EqualityThreshold = 1e-9

func floatingEqual(a, b float64) bool {
	// return math.Abs(a-b) <= float64EqualityThreshold
	return a == b
}

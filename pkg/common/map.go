package common

// MapEqual compares two maps for equality
func MapEqual[K comparable, V comparable](m1, m2 map[K]V) bool {
	if m1 == nil && m2 == nil {
		return true
	}
	if m1 == nil || m2 == nil {
		return false
	}
	if len(m1) != len(m2) {
		return false
	}
	for k, v1 := range m1 {
		v2, exist := m2[k]
		if !exist || v1 != v2 {
			return false
		}
	}
	return true
}

// CloneMap clones a map with comparable keys
func CloneMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return nil
	}
	clone := make(map[K]V, len(m))
	for key, value := range m {
		clone[key] = value
	}
	return clone
}

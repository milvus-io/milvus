package typeutil

// MergeMap merge one map to another
func MergeMap(src map[string]string, dst map[string]string) map[string]string {
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// GetMapKeys return keys of a map
func GetMapKeys(src map[string]string) []string {
	keys := make([]string, len(src))
	i := 0
	for k := range src {
		keys[i] = k
		i++
	}
	return keys
}

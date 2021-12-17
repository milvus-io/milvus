package typeutil

// Add one to string, add one on empty string return empty
func AddOne(data string) string {
	if len(data) == 0 {
		return data
	}
	var datab = []byte(data)
	if datab[len(datab)-1] != 255 {
		datab[len(datab)-1]++
	} else {
		datab = append(datab, byte(0))
	}
	return string(datab)
}

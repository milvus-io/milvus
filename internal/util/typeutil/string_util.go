package typeutil

// AddOne add one to last byte in string, on empty string return empty
// it helps with key iteration upper bound
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

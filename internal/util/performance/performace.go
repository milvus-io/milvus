package performance

import (
	"math"
	"math/rand"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func GenerateData(base float64, iter float64) string {
	multiplier := math.Pow(2, iter)
	length := multiplier * base

	return randStringBytes(int(math.Floor(length)))
}

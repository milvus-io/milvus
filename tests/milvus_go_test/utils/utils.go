package utils

import (
	"math"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func Normalize(d int, v []float32) {
	var norm float32
	for i := 0; i < d; i++ {
		norm += v[i] * v[i]
	}
	norm = float32(math.Sqrt(float64(norm)))
	for i := 0; i < d; i++ {
		v[i] /= norm
	}
}

func GenDefaultValues(nb int, valueType string) []int {
	values := make([]int, nb)
	if valueType == "float" {
		for i := 0; i < nb; i++ {
			values[i] = i + 0.0
		}
	} else {
		for i := 0; i < nb; i++ {
			values[i] = i
		}
	}
	return values
}

func GenFloatVectors(dim int, nb int, normal bool) [][]float32 {
	rand.Seed(time.Now().UnixNano())
	vectors := make([][]float32, nb)
	for i := 0; i < nb; i++ {
		vector := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vector[j] = rand.Float32()
		}
		if normal {
			Normalize(dim, vector)
		}
		vectors[i] = vector
	}
	return vectors
}

func GenBinaryVectors(dim int, nb int) [][]byte {
	rand.Seed(time.Now().UnixNano())
	vectors := make([][]byte, nb)
	for i := 0; i < nb; i++ {
		vector := make([]uint8, dim)
		for j := 0; j < dim; j++ {
			vector[j] = uint8(rand.Intn(2))
		}
		vectors[i] = vector
	}
	return vectors
}

func GenInvalidStrs() []string {
	strs := []string{
		" name ",
		" ",
		"测试",
	}
	return strs
}

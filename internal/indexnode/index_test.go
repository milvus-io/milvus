package indexnode

import "math/rand"

const (
	dim    = 8
	nb     = 10000
	nprobe = 8
)

func generateFloatVectors() []float32 {
	vectors := make([]float32, 0)
	for i := 0; i < nb; i++ {
		for j := 0; j < dim; j++ {
			vectors = append(vectors, rand.Float32())
		}
	}
	return vectors
}

func generateBinaryVectors() []byte {
	vectors := make([]byte, 0)
	for i := 0; i < nb; i++ {
		for j := 0; j < dim/8; j++ {
			vectors = append(vectors, byte(rand.Intn(8)))
		}
	}
	return vectors
}

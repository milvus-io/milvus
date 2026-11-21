package indexparamcheck

import (
	"strings"
	"testing"
)

func TestEMeansParams(t *testing.T) {
	for _, indexType := range []string{"IVF_FLAT", "IVF_PQ", "IVF_SQ8", "IVF_RABITQ", "SCANN"} {
		for _, metric := range []string{"L2", "COSINE"} {
			for _, epochs := range []string{"0", "0.5", "2.5", "5"} {
				if err := validateEMeansParams(indexType, metric, map[string]string{EMeansEpochs: epochs}); err != nil {
					t.Fatalf("%s/%s/%s: %v", indexType, metric, epochs, err)
				}
			}
		}
	}
	if err := validateEMeansParams("HNSW", "IP", nil); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, index, metric, epochs, message string }{
		{"negative", "IVF_FLAT", "L2", "-1", "finite"},
		{"nan", "IVF_FLAT", "L2", "NaN", "finite"},
		{"infinite", "IVF_FLAT", "L2", "Inf", "finite"},
		{"overflow", "IVF_FLAT", "L2", "2147483648", "finite"},
		{"underflow", "IVF_FLAT", "L2", "1e-300", "represented"},
		{"text", "IVF_FLAT", "L2", "five", "finite"},
		{"ip", "IVF_FLAT", "IP", "2.5", "L2 and COSINE"},
		{"binary", "BIN_IVF_FLAT", "HAMMING", "2.5", "CPU IVF"},
		{"hnsw", "HNSW", "L2", "2.5", "CPU IVF"},
		{"gpu", "GPU_IVF_FLAT", "L2", "2.5", "CPU IVF"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateEMeansParams(test.index, test.metric, map[string]string{EMeansEpochs: test.epochs})
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

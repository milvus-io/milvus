package indexparamcheck

import (
	"fmt"
	"math"
	"strconv"
)

// EMeansEpochs selects native e-means for CPU IVF coarse centroid training.
const EMeansEpochs = "e_means_epochs"

func validateEMeansParams(indexType, metric string, params map[string]string) error {
	value, present := params[EMeansEpochs]
	if !present {
		return nil
	}
	epochs, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(epochs) || math.IsInf(epochs, 0) || epochs < 0 || epochs > math.MaxInt32 {
		return fmt.Errorf("%s must be finite and in [0, %d], got %q", EMeansEpochs, math.MaxInt32, value)
	}
	if epochs > 0 && (float32(epochs) == 0 || float64(float32(epochs)) > math.MaxInt32) {
		return fmt.Errorf("%s cannot be represented by the native trainer, got %q", EMeansEpochs, value)
	}
	switch indexType {
	case "IVF_FLAT", "IVFFLAT", "IVF_FLAT_CC", "IVFFLATCC", "IVF_PQ", "IVFPQ",
		"IVF_SQ", "IVFSQ", "IVF_SQ8", "IVF_SQ_CC", "SCANN", "IVF_RABITQ", "IVFRABITQ", "IVF_RABITQ_FASTSCAN":
	default:
		return fmt.Errorf("%s is supported only by CPU IVF indexes, got %s", EMeansEpochs, indexType)
	}
	if epochs > 0 && metric != "L2" && metric != "COSINE" {
		return fmt.Errorf("e-means supports L2 and COSINE, got %s", metric)
	}
	return nil
}

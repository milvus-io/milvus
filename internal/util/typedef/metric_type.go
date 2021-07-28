package typedef

type MetricType = string

const (
	MetricL2             MetricType = "L2"
	MetricIP             MetricType = "IP"
	MetricJaccard        MetricType = "JACCARD"
	MetricTanimoto       MetricType = "TANIMOTO"
	MetricHamming        MetricType = "HAMMING"
	MetricSuperStructure MetricType = "SUPERSTRUCTURE"
	MetricSubStructure   MetricType = "SUBSTRUCTURE"
)

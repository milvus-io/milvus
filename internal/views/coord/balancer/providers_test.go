package balancer

import "github.com/milvus-io/milvus/internal/dataview"

var _ DataViewProvider = (dataview.Manager)(nil)

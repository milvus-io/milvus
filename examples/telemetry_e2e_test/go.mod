module github.com/milvus-io/milvus/examples/telemetry_e2e_test

go 1.24

require (
	github.com/milvus-io/milvus-proto/go-api/v2 v2.6.6-0.20251215075310-deda9c0dcece
	github.com/milvus-io/milvus/client/v2 v2.6.4-0.20251104142533-a2ce70d25256
	google.golang.org/grpc v1.71.0
)

replace (
	github.com/milvus-io/milvus/client/v2 => ../../client
	github.com/milvus-io/milvus/pkg/v2 => ../../pkg
)

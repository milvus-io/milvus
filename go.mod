module github.com/milvus-io/milvus

go 1.16

require (
	github.com/BurntSushi/toml v1.0.0
	github.com/HdrHistogram/hdrhistogram-go v1.0.1 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20210826220005-b48c857c3a0e
	github.com/antonmedv/expr v1.8.9
	github.com/apache/arrow/go/v8 v8.0.0-20220322092137-778b1772fd20
	github.com/apache/pulsar-client-go v0.6.1-0.20210728062540-29414db801a7
	github.com/apache/thrift v0.15.0
	github.com/bits-and-blooms/bloom/v3 v3.0.1
	github.com/confluentinc/confluent-kafka-go v1.8.2
	github.com/containerd/cgroups v1.0.2
	github.com/facebookgo/ensure v0.0.0-20200202191622-63f1cf65ac4c // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4 // indirect
	github.com/gin-gonic/gin v1.7.7
	github.com/go-basic/ipv4 v1.0.0
	github.com/gofrs/flock v0.8.1
	github.com/golang/mock v1.5.0
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/jarcoal/httpmock v1.0.8
	github.com/klauspost/compress v1.14.2
	github.com/lingdor/stackerror v0.0.0-20191119040541-976d8885ed76
	github.com/minio/minio-go/v7 v7.0.10
	github.com/opentracing/opentracing-go v1.2.0
	github.com/panjf2000/ants/v2 v2.4.8
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/sbinet/npyio v0.6.0
	github.com/shirou/gopsutil v3.21.8+incompatible
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.3.1
	github.com/spf13/viper v1.8.1
	github.com/streamnative/pulsarctl v0.5.0
	github.com/stretchr/testify v1.7.0
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.etcd.io/etcd/server/v3 v3.5.0
	go.uber.org/atomic v1.7.0
	go.uber.org/automaxprocs v1.4.0
	go.uber.org/zap v1.17.0
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/exp v0.0.0-20220303212507-bbda1eaf7a17
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/grpc v1.44.0
	google.golang.org/grpc/examples v0.0.0-20220617181431-3e7b97febc7f
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	stathat.com/c/consistent v1.0.0
)

replace (
	github.com/apache/pulsar-client-go => github.com/milvus-io/pulsar-client-go v0.6.8
	github.com/bketelsen/crypt => github.com/bketelsen/crypt v0.0.4 // Fix security alert for core-os/etcd
	github.com/dgrijalva/jwt-go => github.com/golang-jwt/jwt v3.2.2+incompatible // Fix security alert for jwt-go 3.2.0
	github.com/go-kit/kit => github.com/go-kit/kit v0.1.0
	github.com/streamnative/pulsarctl => github.com/xiaofan-luan/pulsarctl v0.0.2
)

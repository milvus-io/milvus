module github.com/milvus-io/milvus

go 1.15

require (
	github.com/HdrHistogram/hdrhistogram-go v1.0.1 // indirect
	github.com/antonmedv/expr v1.8.9
	github.com/apache/pulsar-client-go v0.5.0
	github.com/apache/thrift/lib/go/thrift v0.0.0-20210120171102-e27e82c46ba4
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/facebookgo/ensure v0.0.0-20200202191622-63f1cf65ac4c // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4 // indirect
	github.com/frankban/quicktest v1.10.2 // indirect
	github.com/go-basic/ipv4 v1.0.0
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.4.3
	github.com/google/btree v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/jarcoal/httpmock v1.0.8
	github.com/klauspost/compress v1.10.11 // indirect
	github.com/minio/minio-go/v7 v7.0.10
	github.com/mitchellh/mapstructure v1.1.2
	github.com/opentracing-contrib/go-grpc v0.0.0-20200813121455-4a6760c71486
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/quasilyte/go-ruleguard v0.2.1 // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.3.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.6.1
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/yahoo/athenz v1.9.16 // indirect
	go.etcd.io/etcd v3.3.25+incompatible
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20201202161906-c7110b5ffcbb
	golang.org/x/text v0.3.3
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	google.golang.org/grpc v1.31.0
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace (
	github.com/apache/pulsar-client-go => github.com/apache/pulsar-client-go v0.5.0
	github.com/coreos/etcd => github.com/ozonru/etcd v3.3.20-grpc1.27-origmodule+incompatible
	go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5

	//google.golang.org/api => google.golang.org/api v0.14.0

	//replace go.etcd.io/etcd => go.etcd.io/etcd v0.5.0-alpha.5.0.20200329194405-dd816f0735f8
	google.golang.org/grpc => google.golang.org/grpc v1.27.0
)

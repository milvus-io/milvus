module github.com/zilliztech/milvus-distributed

go 1.15

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20200131002437-cf55d5288a48 // indirect
	github.com/apache/pulsar-client-go v0.1.1
	github.com/aws/aws-sdk-go v1.30.8
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/frankban/quicktest v1.10.2 // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/btree v1.0.0
	github.com/klauspost/compress v1.10.11 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/minio/minio-go/v7 v7.0.5
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/onsi/ginkgo v1.12.1 // indirect
	github.com/onsi/gomega v1.10.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712 // indirect
	github.com/pingcap/errors v0.11.4 // indirect
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463 // indirect
	github.com/pivotal-golang/bytefmt v0.0.0-20200131002437-cf55d5288a48
	github.com/prometheus/client_golang v1.5.1 // indirect
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.3.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.6.1
	github.com/tikv/client-go v0.0.0-20200824032810-95774393107b
	github.com/yahoo/athenz v1.9.16 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20200904194848-62affa334b73 // indirect
	golang.org/x/sys v0.0.0-20200905004654-be1d3432aa8f // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	golang.org/x/tools v0.0.0-20200825202427-b303f430e36d // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto v0.0.0-20200122232147-0452cf42e150 // indirect
	google.golang.org/grpc v1.31.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v2 v2.3.0
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace (
	github.com/coreos/etcd => github.com/ozonru/etcd v3.3.20-grpc1.27-origmodule+incompatible
	go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5

	google.golang.org/api => google.golang.org/api v0.14.0

	//replace go.etcd.io/etcd => go.etcd.io/etcd v0.5.0-alpha.5.0.20200329194405-dd816f0735f8
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)

module github.com/czs007/suvlim

go 1.15

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20200131002437-cf55d5288a48 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/apache/pulsar/pulsar-client-go v0.0.0-20200901051823-800681aaa9af
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/docker/go-units v0.4.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/google/btree v1.0.0
	github.com/minio/minio-go/v7 v7.0.5
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.4 // indirect
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pivotal-golang/bytefmt v0.0.0-20200131002437-cf55d5288a48
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.6.1
	github.com/tikv/client-go v0.0.0-20200824032810-95774393107b
	github.com/tikv/pd v2.1.19+incompatible
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.15.0
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	google.golang.org/grpc v1.31.1
	google.golang.org/grpc/examples v0.0.0-20200828165940-d8ef479ab79a // indirect
	google.golang.org/protobuf v1.25.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5

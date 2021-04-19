module github.com/czs007/suvlim

go 1.15

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20200131002437-cf55d5288a48 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/apache/pulsar-client-go v0.2.0
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20200825011529-c078454b47b6 // indirect
	github.com/apache/pulsar/pulsar-client-go v0.0.0-20200901051823-800681aaa9af
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/danieljoos/wincred v1.1.0 // indirect
	github.com/docker/go-units v0.4.0
	github.com/dvsekhvalnov/jose2go v0.0.0-20200901110807-248326c1351b // indirect
	github.com/frankban/quicktest v1.10.2 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/google/btree v1.0.0
	github.com/json-iterator/go v1.1.10
	github.com/keybase/go-keychain v0.0.0-20200502122510-cda31fe0c86d // indirect
	github.com/klauspost/compress v1.10.11 // indirect
	github.com/minio/minio-go/v7 v7.0.5
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.4 // indirect
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pivotal-golang/bytefmt v0.0.0-20200131002437-cf55d5288a48
	github.com/prometheus/common v0.13.0 // indirect
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.6.1
	github.com/tikv/client-go v0.0.0-20200824032810-95774393107b
	github.com/tikv/pd v2.1.19+incompatible
	github.com/yahoo/athenz v1.9.16 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a // indirect
	golang.org/x/net v0.0.0-20200904194848-62affa334b73 // indirect
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43 // indirect
	golang.org/x/sys v0.0.0-20200905004654-be1d3432aa8f // indirect
	google.golang.org/grpc v1.31.0
	google.golang.org/grpc/examples v0.0.0-20200828165940-d8ef479ab79a // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.3.0
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace go.etcd.io/bbolt => go.etcd.io/bbolt v1.3.5

//replace go.etcd.io/etcd => go.etcd.io/etcd v0.5.0-alpha.5.0.20200329194405-dd816f0735f8
replace google.golang.org/grpc => google.golang.org/grpc v1.26.0

replace github.com/coreos/etcd => github.com/ozonru/etcd v3.3.20-grpc1.27-origmodule+incompatible

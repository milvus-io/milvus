package indexservice

import (
	"net"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int

	MasterAddress string

	EtcdAddress  string
	KvRootPath   string
	MetaRootPath string

	MinIOAddress         string
	MinIOAccessKeyID     string
	MinIOSecretAccessKey string
	MinIOUseSSL          bool
	MinioBucketName      string
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		pt.initAddress()
		pt.initPort()
		pt.initEtcdAddress()
		pt.initMasterAddress()
		pt.initMetaRootPath()
		pt.initKvRootPath()
		pt.initMinIOAddress()
		pt.initMinIOAccessKeyID()
		pt.initMinIOSecretAccessKey()
		pt.initMinIOUseSSL()
		pt.initMinioBucketName()
	})
}

func (pt *ParamTable) initAddress() {
	addr, err := pt.Load("indexService.address")
	if err != nil {
		panic(err)
	}

	hostName, _ := net.LookupHost(addr)
	if len(hostName) <= 0 {
		if ip := net.ParseIP(addr); ip == nil {
			panic("invalid ip indexServer.address")
		}
	}

	port, err := pt.Load("indexService.port")
	if err != nil {
		panic(err)
	}
	_, err = strconv.Atoi(port)
	if err != nil {
		panic(err)
	}

	pt.Address = addr + ":" + port
}

func (pt *ParamTable) initPort() {
	pt.Port = pt.ParseInt("indexService.port")
}

func (pt *ParamTable) initEtcdAddress() {
	addr, err := pt.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}
	pt.EtcdAddress = addr
}

func (pt *ParamTable) initMetaRootPath() {
	rootPath, err := pt.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := pt.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	pt.MetaRootPath = rootPath + "/" + subPath
}

func (pt *ParamTable) initKvRootPath() {
	rootPath, err := pt.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := pt.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	pt.KvRootPath = rootPath + "/" + subPath
}

func (pt *ParamTable) initMasterAddress() {
	ret, err := pt.Load("_MasterAddress")
	if err != nil {
		panic(err)
	}
	pt.MasterAddress = ret
}

func (pt *ParamTable) initMinIOAddress() {
	ret, err := pt.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	pt.MinIOAddress = ret
}

func (pt *ParamTable) initMinIOAccessKeyID() {
	ret, err := pt.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	pt.MinIOAccessKeyID = ret
}

func (pt *ParamTable) initMinIOSecretAccessKey() {
	ret, err := pt.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	pt.MinIOSecretAccessKey = ret
}

func (pt *ParamTable) initMinIOUseSSL() {
	ret, err := pt.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	pt.MinIOUseSSL, err = strconv.ParseBool(ret)
	if err != nil {
		panic(err)
	}
}

func (pt *ParamTable) initMinioBucketName() {
	bucketName, err := pt.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	pt.MinioBucketName = bucketName
}

package paramtable

import "github.com/milvus-io/milvus/pkg/util/etcd"

func GetEtcdCfg(etcdConfig *EtcdConfig) *etcd.EtcdCfg {
	return &etcd.EtcdCfg{
		UseEmbed:           etcdConfig.UseEmbedEtcd.GetAsBool(),
		EnableAuth:         etcdConfig.EtcdEnableAuth.GetAsBool(),
		UserName:           etcdConfig.EtcdAuthUserName.GetValue(),
		PassWord:           etcdConfig.EtcdAuthPassword.GetValue(),
		UseSSL:             etcdConfig.EtcdUseSSL.GetAsBool(),
		Endpoints:          etcdConfig.Endpoints.GetAsStrings(),
		CertFile:           etcdConfig.EtcdTLSCert.GetValue(),
		KeyFile:            etcdConfig.EtcdTLSKey.GetValue(),
		CaCertFile:         etcdConfig.EtcdTLSCACert.GetValue(),
		MinVersion:         etcdConfig.EtcdTLSMinVersion.GetValue(),
		KeyPrefix:          etcdConfig.RootPath.GetValue(),
		MaxRPCSendMsgBytes: etcdConfig.MaxRPCSendMsgBytes.GetAsInt(),
		MaxRPCRecvMsgBytes: etcdConfig.MaxRPCRecvMsgBytes.GetAsInt(),
	}
}

# How to start a master

## Requirements
### Start a etcdv3
```
./etcd -listen-peer-urls=http://192.168.1.10:12380 -advertise-client-urls=http://192.168.1.10:12379 -listen-client-urls http://0.0.0.0:12379,http://0.0.0.0:14001 -initial-advertise-peer-urls=http://192.168.1.10:12380
```
## Start from code
```
go run cmd/master.go
```

## Start with docker

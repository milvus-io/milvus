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


## What rules does master use to write data to kv storage?
1.find the root path variable ```ETCD_ROOT_PATH ```which defined in common/config.go 
2.add prefix path ```segment``` if the resource is a segement
3.add prefix path ```collection``` if the resource is a collection
4.add resource uuid

### example
if master create a collection with uuid  ```46e468ee-b34a-419d-85ed-80c56bfa4e90```
the corresponding key in etcd is $(ETCD_ROOT_PATH)/collection/46e468ee-b34a-419d-85ed-80c56bfa4e90

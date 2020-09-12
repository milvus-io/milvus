master-proto-gen:
	protoc --go_out=plugins=grpc,paths=source_relative:. pkg/master/grpc/master/master.proto
	protoc --go_out=plugins=grpc,paths=source_relative:. pkg/master/grpc/message/message.proto

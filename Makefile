master-proto-gen:
	${protoc} --go_out=plugins=grpc,paths=source_relative:. internal/proto/master/master.proto
	${protoc} --go_out=plugins=grpc,paths=source_relative:. internal/proto/message/message.proto

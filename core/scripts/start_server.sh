#!/bin/bash
export GRPC_VERBOSITY=DEBUG
export GRPC_TRACE=call_error,op_failure,client_channel_call,client_channel_routing
../bin/milvus_server -c ../conf/server_config.yaml


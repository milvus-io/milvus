cd ../build/docker/deploy/

echo "starting master docker"
nohup docker-compose -p milvus up master > ~/master_docker.log 2>&1 &

echo "starting proxynode docker"
nohup docker-compose -p milvus up  proxynode > ~/proxynode_docker.log 2>&1 &

echo "starting indexservice docker"
nohup docker-compose -p milvus up  indexservice > ~/indexservice_docker.log 2>&1 &

echo "starting indexnode docker"
nohup docker-compose -p milvus up  indexnode > ~/indexnode_docker.log 2>&1 &

echo "starting queryservice docker"
nohup docker-compose -p milvus up queryservice > ~/queryservice_docker.log 2>&1 &

echo "starting dataservice docker"
nohup docker-compose -p milvus up dataservice > ~/dataservice_docker.log 2>&1 &

echo "starting querynode1 docker"
nohup docker-compose -p milvus run -e QUERY_NODE_ID=1 querynode > ~/querynode1_docker.log 2>&1 &

echo "starting querynode2 docker"
nohup docker-compose -p milvus run -e QUERY_NODE_ID=2 querynode > ~/querynode2_docker.log 2>&1 &

echo "starting datanode docker"
nohup docker-compose -p milvus run -e DATA_NODE_ID=3 datanode > ~/datanode_docker.log 2>&1 &

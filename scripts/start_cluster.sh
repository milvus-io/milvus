cd ..

echo "Starting rootcoord..."
nohup ./bin/milvus run rootcoord > /tmp/rootcoord.log 2>&1 &

echo "Starting datacoord..."
nohup ./bin/milvus run datacoord > /tmp/datacoord.log 2>&1 &

echo "Starting datanode..."
nohup ./bin/milvus run datanode > /tmp/datanode.log 2>&1 &

echo "Starting proxy..."
nohup ./bin/milvus run proxy > /tmp/proxy.log 2>&1 &

echo "Starting querycoord..."
nohup ./bin/milvus run querycoord > /tmp/querycoord.log 2>&1 &

echo "Starting querynode..."
nohup ./bin/milvus run querynode > /tmp/querynode.log 2>&1 &

echo "Starting indexcoord..."
nohup ./bin/milvus run indexcoord > /tmp/indexcoord.log 2>&1 &

echo "Starting indexnode..."
nohup ./bin/milvus run indexnode > /tmp/indexnode.log 2>&1 &
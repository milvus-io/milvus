cd ..

echo "starting rootcoord"
nohup ./bin/milvus run rootcoord > ~/rootcoord.log 2>&1 &

echo "starting datacoord"
nohup ./bin/milvus run datacoord > ~/datacoord.log 2>&1 &

echo "starting datanode"
nohup ./bin/milvus run datanode > ~/datanode.log 2>&1 &

echo "starting proxy"
nohup ./bin/milvus run proxy > ~/proxy.log 2>&1 &

echo "starting querycoord"
nohup ./bin/milvus run querycoord > ~/querycoord.log 2>&1 &

echo "starting querynode1"
nohup ./bin/milvus run querynode > ~/querynode1.log 2>&1 &

echo "starting indexcoord"
nohup ./bin/milvus run indexcoord > ~/indexcoord.log 2>&1 &

echo "starting indexnode"
nohup ./bin/milvus run indexnode > ~/indexnode.log 2>&1 &

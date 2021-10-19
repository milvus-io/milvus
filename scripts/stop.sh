echo "stopping milvus"
kill -9 $(ps -e | grep milvus | awk '{print $1}')
echo "milvs stopped"


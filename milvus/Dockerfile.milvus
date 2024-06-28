# Use an appropriate base image
FROM milvusdb/milvus:latest

# Expose the necessary port
EXPOSE 19530

# Set environment variables
ENV ETCD_ENDPOINTS=etcd:2379
ENV MINIO_ADDRESS=minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin

# Start Milvus
CMD ["milvus", "run", "standalone"]

# Use an appropriate base image
FROM minio/minio:RELEASE.2023-03-20T20-16-18Z

# Expose the necessary port
EXPOSE 9000

# Set environment variables
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin

# Start MinIO
CMD ["minio", "server", "/data"]

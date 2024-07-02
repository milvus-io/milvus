# Use an appropriate base image
FROM quay.io/coreos/etcd:v3.4.13

# Expose the necessary ports
EXPOSE 2379 2380

# Start ETCD with the required configuration
CMD ["etcd", "--listen-client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://0.0.0.0:2379", "--listen-peer-urls=http://0.0.0.0:2380", "--initial-advertise-peer-urls=http://0.0.0.0:2380", "--initial-cluster", "default=http://0.0.0.0:2380"]

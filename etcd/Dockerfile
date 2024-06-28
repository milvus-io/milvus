# Use the official etcd image
FROM quay.io/coreos/etcd:v3.4.13

# Create a health check script
RUN echo '#!/bin/sh' > /healthcheck.sh \
    && echo 'wget -qO- http://localhost:2379/health' >> /healthcheck.sh \
    && chmod +x /healthcheck.sh

# Expose the necessary ports
EXPOSE 2379 2380

# Start ETCD with the required configuration and run the health check script
CMD ["sh", "-c", "etcd --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://0.0.0.0:2379 --listen-peer-urls=http://0.0.0.0:2380 --initial-advertise-peer-urls=http://0.0.0.0:2380 --initial-cluster=default=http://0.0.0.0:2380 & /healthcheck.sh"]

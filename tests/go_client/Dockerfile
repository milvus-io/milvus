FROM golang:1.26.2 as builder

# Define a build argument with an empty default value
ARG CUSTOM_GOPROXY=""

# Set the GOPROXY environment variable, using the specified value if provided, or a default if not
ENV GOPROXY=${CUSTOM_GOPROXY:-https://proxy.golang.org}

RUN go install gotest.tools/gotestsum@v1.13.0

# Install Python3 (3.13 in Debian Trixie) for external table test data generation.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
# Required packages for external table tests (must succeed)
RUN pip3 install --no-cache-dir --break-system-packages \
    pyarrow==23.0.1 lance==1.2.1 obstore==0.9.1
# Optional: vortex-data has Rust native deps that may not build on all platforms.
# Version must match the Rust vortex crate version in milvus-storage.
RUN pip3 install --no-cache-dir --break-system-packages \
    vortex-data==0.56.0 || true

# Set the Current Working Directory inside the container
WORKDIR /milvus

# Copy go mod and sum files
COPY client/go.mod client/go.mod
COPY client/go.sum client/go.sum
COPY tests/go_client/go.mod tests/go_client/
COPY tests/go_client/go.sum tests/go_client/

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN cd tests/go_client && go mod download

# Copy the source code into the container
COPY client client
COPY tests/go_client tests/go_client

WORKDIR /milvus/tests/go_client

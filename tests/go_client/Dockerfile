FROM golang:1.24.2 as builder

# Define a build argument with an empty default value
ARG CUSTOM_GOPROXY=""

# Set the GOPROXY environment variable, using the specified value if provided, or a default if not
ENV GOPROXY=${CUSTOM_GOPROXY:-https://proxy.golang.org}

RUN go install gotest.tools/gotestsum@v1.12.0

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

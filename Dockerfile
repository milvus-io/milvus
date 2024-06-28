# Start with an official Docker image
FROM docker:latest

# Install Docker Compose
RUN apk add --no-cache docker-compose

# Set the working directory
WORKDIR /app

# Copy the docker-compose.yml into the image
COPY docker-compose.yml /app/docker-compose.yml

# Ensure Docker Daemon is running
RUN mkdir /var/run/docker.sock

# Set the entrypoint to use docker-compose to start the services
CMD ["docker-compose", "up"]

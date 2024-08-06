
# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if docker-compose exists
if command_exists docker-compose; then
    echo "Using docker-compose"
    DOCKER_COMPOSE_COMMAND="docker-compose"
else
    echo "Using docker compose"
    DOCKER_COMPOSE_COMMAND="docker compose"
fi

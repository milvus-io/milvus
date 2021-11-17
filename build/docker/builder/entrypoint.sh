#!/bin/bash

# Exit immediately for non zero status
set -e

# Ensure $HOME exists when starting
if [ ! -d "${HOME}" ]; then
  mkdir -p "${HOME}"
fi

# Setup $PS1 for a consistent and reasonable prompt
if [ -w "${HOME}" ] && [ -d /etc/skel ]; then
  cp /etc/skel/.bash* "${HOME}"
fi

# Add current (arbitrary) user to /etc/passwd and /etc/group
if ! whoami &> /dev/null; then
  if [ -w /etc/passwd ]; then
    echo "${USER_NAME:-user}:x:$(id -u):0:${USER_NAME:-user} user:${HOME}:/bin/bash" >> /etc/passwd
    echo "${USER_NAME:-user}:x:$(id -u):" >> /etc/group
  fi
fi

set +e
if [ -f "/etc/profile.d/devtoolset-7.sh" ]; then
  source "/etc/profile.d/devtoolset-7.sh"
fi

if [ -f "/etc/profile.d/llvm-toolset-7.sh" ]; then
  source "/etc/profile.d/llvm-toolset-7.sh"
fi

# Exit immediately for non zero status
set -e

exec "$@"

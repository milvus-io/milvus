#!/bin/bash
set -euo pipefail

# Usage message
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <target_directory>"
    exit 1
fi

TARGET_DIR="$1"

# Ensure that the raw key is provided via STDIN.
# If the script is run interactively (no data in STDIN), complain.
if [ -t 0 ]; then
    echo "Error: please provide the raw key via standard input."
    echo "Example: head -c 32 /dev/urandom | $0 $TARGET_DIR"
    exit 1
fi

# Read the raw key from STDIN (it may be a long string; be sure not to add extra spaces)
RAW_KEY=$(cat)

# Check that fscrypt is installed
if ! command -v fscrypt >/dev/null 2>&1; then
    echo "Error: fscrypt command not found. Please install fscrypt."
    exit 1
fi


##############################################
# 1. Global Setup and Filesystem Setup
##############################################

# Check and initialize global fscrypt configuration if necessary.
# (fscrypt setup prints a notice if already set up; using --quiet avoids redundant output.)
echo "Setting up global fscrypt configuration (if not already set up)..."
fscrypt setup --quiet 2>/dev/null || true

# Determine the mount point of the target directory.
# Using findmnt to get the mounted filesystem root.
MOUNT_POINT=$(findmnt -n --target "$TARGET_DIR" -o TARGET || true)
if [ -z "$MOUNT_POINT" ]; then
    # If the target directory does not yet exist, determine it from its parent.
    MOUNT_POINT=$(findmnt -n --target "$(dirname "$TARGET_DIR")" -o TARGET)
fi
DEV=$(findmnt -n --output=SOURCE --target $MOUNT_POINT)
echo "Mount point: $MOUNT_POINT"
echo "Device: $DEV"

# Check if the mount point is already set up for fscrypt.
if [ ! -d "$MOUNT_POINT/.fscrypt" ]; then
    echo "Filesystem at mount point '$MOUNT_POINT' is not set up for fscrypt."

    echo "Setting up fscrypt on filesystem '$MOUNT_POINT'..."
    fscrypt setup "$MOUNT_POINT" --quiet --time=1ms
    chown -R root:root "$MOUNT_POINT/.fscrypt"
    chmod -R 700 "$MOUNT_POINT/.fscrypt"
fi

##############################################
# 2. Ensure target directory exists
##############################################
if [ ! -d "$TARGET_DIR" ]; then
    echo "Directory '$TARGET_DIR' does not exist. Creating it..."
    mkdir -p "$TARGET_DIR"
fi

# If TargetDir is not owned by root
chown -R root:root "$TARGET_DIR"

##############################################
# 3. Evaluate Encryption State and Act Accordingly
##############################################

# Capture the output of fscrypt status for TARGET_DIR.
# The output is expected to include phrases indicating whether the directory is
STATUS_OUTPUT=$(fscrypt status "$TARGET_DIR" 2>&1 || true)
echo "Status output:" $STATUS_OUTPUT

if echo $STATUS_OUTPUT | grep -qi "encryption not enabled on filesystem"; then
    echo "Enabling encryption on filesystem $DEV"
    # https://github.com/google/fscrypt?tab=readme-ov-file#getting-encryption-not-enabled-on-an-ext4-filesystem
    tune2fs -O encrypt "$DEV"
    STATUS_OUTPUT=$(fscrypt status "$TARGET_DIR" 2>&1 || true)
    echo "Status output:" $STATUS_OUTPUT
fi

if echo $STATUS_OUTPUT | grep -qi "not encrypted"; then
    echo "Directory '$TARGET_DIR' is not encrypted."
    echo "Encrypting directory using the provided raw key..."
    # Use the raw key from STDIN to encrypt the directory.
    echo "${RAW_KEY}" | fscrypt encrypt "${TARGET_DIR}" --source=raw_key --name "milvus_admin" --quiet
elif echo ${STATUS_OUTPUT} | grep -qi "Unlocked: No"; then
   echo "Directory '${TARGET_DIR}' is encrypted but locked."
   exit 1
else
    echo "Directory '$TARGET_DIR' is already encrypted and unlocked. No action needed."
fi

echo "Operation completed successfully."

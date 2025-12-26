#!/bin/sh
set -e

echo "[entrypoint] Starting foiacquire..."
echo "[entrypoint] TARGET_PATH=$TARGET_PATH"
echo "[entrypoint] Command: $@"

# If running as root and USER_ID is set, adjust the foiacquire user's UID/GID
if [ "$(id -u)" = "0" ]; then
    echo "[entrypoint] Running as root, checking UID/GID settings..."

    # Adjust group ID if specified
    if [ -n "$GROUP_ID" ] && [ "$GROUP_ID" != "$(id -g foiacquire)" ]; then
        echo "[entrypoint] Setting GID to $GROUP_ID"
        groupmod -g "$GROUP_ID" foiacquire
    fi

    # Adjust user ID if specified
    if [ -n "$USER_ID" ] && [ "$USER_ID" != "$(id -u foiacquire)" ]; then
        echo "[entrypoint] Setting UID to $USER_ID"
        usermod -u "$USER_ID" foiacquire
    fi

    # Fix ownership of the data directory
    echo "[entrypoint] Fixing ownership of $TARGET_PATH"
    chown -R foiacquire:foiacquire "$TARGET_PATH"

    # Drop privileges and run as foiacquire
    echo "[entrypoint] Dropping to user foiacquire..."
    exec su-exec foiacquire foiacquire --target "$TARGET_PATH" "$@"
else
    # Already running as non-root, just exec
    echo "[entrypoint] Running as non-root (uid=$(id -u))"
    exec foiacquire --target "$TARGET_PATH" "$@"
fi

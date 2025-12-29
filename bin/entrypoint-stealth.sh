#!/bin/sh
chromium-browser \
    --headless \
    --no-sandbox \
    --disable-gpu \
    --disable-dev-shm-usage \
    --disable-software-rasterizer \
    --disable-blink-features=AutomationControlled \
    --disable-infobars \
    --disable-background-networking \
    --disable-sync \
    --disable-translate \
    --no-first-run \
    --no-default-browser-check \
    --remote-debugging-port=9223 \
    "$@" &
sleep 2
exec socat TCP-LISTEN:9222,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:9223

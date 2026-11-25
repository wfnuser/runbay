#!/bin/sh
# Install Runbay's own statically linked helper. No setuid bit or file capabilities.
set -eu
base=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT
sh "$base/scripts/build-linux-launcher.sh" "$build/runbay-landlock"
install -d -m 0755 /usr/local/libexec
install -m 0755 "$build/runbay-landlock" /usr/local/libexec/runbay-landlock
if [ "$(cat /proc/sys/kernel/apparmor_restrict_unprivileged_userns 2>/dev/null || true)" = 1 ]; then
    apparmor_parser -r "$base/scripts/apparmor/runbay-landlock"
    install -m 0644 "$base/scripts/apparmor/runbay-landlock" /etc/apparmor.d/runbay-landlock
fi

#!/bin/sh
# Build the same static launcher and source identity as the Cargo build script.
set -eu
native=$(CDPATH='' cd -- "$(dirname -- "$0")/../crates/runbay-sandbox/native" && pwd)
id=$(cat "$native/launcher.c" "$native/policy.h" | sha256sum)
exec "${CC:-cc}" -std=gnu11 -O2 -Wall -Wextra -Werror -static \
    "-DRUNBAY_POLICY_ID=\"${id%% *}\"" "$native/launcher.c" -o "$1"

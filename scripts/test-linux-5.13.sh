#!/bin/sh
# KERNEL_IMAGE must name a trusted bootable x86_64 Linux 5.13 bzImage with Landlock enabled.
set -eu
: "${KERNEL_IMAGE:?Set KERNEL_IMAGE to a Linux 5.13 x86_64 bzImage}"
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT
mkdir -p "$build/image/dev"
cc=${KERNEL_CC:-x86_64-linux-gnu-gcc}
CC="$cc" sh "$repo/scripts/build-linux-launcher.sh" "$build/image/runbay-landlock"
"$cc" -O2 -Wall -Wextra -Werror -static "$repo/crates/runbay-sandbox/tests/kernel/init.c" -o "$build/image/init"
(cd "$build/image" && find . | cpio --quiet -o -H newc) | gzip > "$build/root.cpio.gz"
if ! timeout 100 qemu-system-x86_64 -accel tcg -cpu max -m 384 -smp 1 -nic none -nographic -no-reboot \
    -kernel "$KERNEL_IMAGE" -initrd "$build/root.cpio.gz" \
    -append 'console=ttyS0 quiet panic=-1 rdinit=/init lsm=landlock,capability' > "$build/serial" 2>&1; then
    cat "$build/serial"
    exit 1
fi
cat "$build/serial"
grep -q 'RUNBAY PASS Linux 5.13 native sandbox and UDS' "$build/serial"

#!/bin/sh
# Run inside a fresh systemd service with Delegate=yes (see README).
set -eu
relative=$(sed -n 's/^0:://p' /proc/self/cgroup)
case "$relative" in /*.service) ;; *) echo "Expected a dedicated cgroup v2 service" >&2; exit 1 ;; esac
parent="/sys/fs/cgroup$relative"
# Refuse a shared service: enabling domain controllers requires an empty parent.
if ! { read -r first && [ "$first" = "$$" ] && ! read -r more; } < "$parent/cgroup.procs"; then
    echo "The delegated service must contain only this launcher" >&2
    exit 1
fi
mkdir "$parent/supervisor"
printf '%s\n' "$$" > "$parent/supervisor/cgroup.procs"
printf '%s\n' '+cpu +memory +pids' > "$parent/cgroup.subtree_control"
export RUNBAY_CGROUP_ROOT="$parent"
exec "$@"

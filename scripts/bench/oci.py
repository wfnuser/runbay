"""Run the same read probe in a fresh, network-isolated crun container per sample."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("benchmark", type=Path)
    parser.add_argument("static_probe", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as directory:
        bundle = Path(directory)
        root = bundle / "rootfs"
        root.mkdir()
        (root / "dev").mkdir()
        shutil.copyfile(args.static_probe, root / "probe")
        (root / "probe").chmod(0o755)
        config = {
            "ociVersion": "1.0.2", "root": {"path": "rootfs", "readonly": True},
            "process": {"terminal": False, "user": {"uid": 65534, "gid": 65534},
                        "args": ["/probe"], "cwd": "/", "noNewPrivileges": True},
            "mounts": [{"destination": "/dev", "type": "tmpfs", "source": "tmpfs",
                        "options": ["nosuid", "mode=755"]}],
            "linux": {"namespaces": [{"type": kind} for kind in ("pid", "mount", "ipc", "uts", "network")]},
        }
        (bundle / "config.json").write_text(json.dumps(config))
        subprocess.run([args.benchmark.resolve(), "external", "crun", "--root", bundle / "state",
                        "--cgroup-manager=disabled", "run", "--bundle", bundle, "runbay-bench"], check=True)


if __name__ == "__main__":
    main()

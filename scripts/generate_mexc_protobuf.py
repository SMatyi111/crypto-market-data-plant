"""Regenerate the vendored MEXC protobuf Python bindings (dev/build-time only).

Runs `protoc` (via grpcio-tools, NOT a runtime dependency) over the vendored
`.proto` files and writes the `*_pb2.py` modules into the committed
`crypto_collector/collectors/mexc_pb/` package, then rewrites the wrapper's
cross-module imports to be package-relative so they resolve inside that package
without putting the proto dir on `sys.path`.

Runtime needs only the `protobuf` runtime - never `protoc`. See
`src/crypto_collector/proto/mexc/README.md`.

Usage (from the repo root):

    pip install grpcio-tools
    python scripts/generate_mexc_protobuf.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PROTO_DIR = _REPO_ROOT / "src" / "crypto_collector" / "proto" / "mexc"
_OUT_DIR = _REPO_ROOT / "src" / "crypto_collector" / "collectors" / "mexc_pb"

# protoc emits top-level `import Foo_pb2 as Foo__pb2`; rewrite to package-relative
# so the vendored bindings import each other inside the mexc_pb package.
_TOP_LEVEL_IMPORT = re.compile(r"^import (\w+_pb2) as (\w+__pb2)$", re.MULTILINE)


def main() -> int:
    protos = sorted(_PROTO_DIR.glob("*.proto"))
    if not protos:
        print(f"no .proto files found in {_PROTO_DIR}", file=sys.stderr)
        return 1
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{_PROTO_DIR}",
        f"--python_out={_OUT_DIR}",
        *[str(p) for p in protos],
    ]
    print("running:", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(
            "protoc failed - install the dev toolchain first: pip install grpcio-tools",
            file=sys.stderr,
        )
        return result.returncode

    for pb2 in _OUT_DIR.glob("*_pb2.py"):
        text = pb2.read_text(encoding="utf-8")
        rewritten = _TOP_LEVEL_IMPORT.sub(r"from . import \1 as \2", text)
        if rewritten != text:
            pb2.write_text(rewritten, encoding="utf-8")
            print(f"rewrote package-relative imports in {pb2.name}")

    print(f"generated {len(protos)} module(s) into {_OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

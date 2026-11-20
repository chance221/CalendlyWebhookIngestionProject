"""AWS Glue PySpark transformation jobs and Pydantic schema contracts."""

from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path


def _extract_native_wheels_from_zip() -> None:
    """If AWS Glue mounted `glue_dependencies.zip` via `--extra-py-files`, extract it to
    a local directory on the container filesystem so Linux `dlopen()` can load compiled
    C/Rust `.so` extensions (such as `pydantic_core._pydantic_core`).
    """
    for entry in list(sys.path):
        if entry.endswith("glue_dependencies.zip") and Path(entry).is_file():
            extract_dir = Path(tempfile.gettempdir()) / "calendly_glue_deps"
            marker = extract_dir / ".extracted"
            if not marker.exists():
                extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(entry, "r") as zf:
                    zf.extractall(extract_dir)
                marker.write_text("ok", encoding="utf-8")
            if str(extract_dir) not in sys.path:
                sys.path.insert(0, str(extract_dir))


_extract_native_wheels_from_zip()

